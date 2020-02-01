library(data.table)
library(magrittr)

#### raw data ####

meta = fread("meta.csv")

cold_start_test = fread("cold_start_test.csv")
cold_start_test$V1 = NULL
consumption_train = fread("consumption_train.csv")
consumption_train$V1 = NULL

submission_format = fread("submission_format.csv")
weekday_dict = setNames(colnames(meta)[4:10], c(2:7, 1))

prediction_window_dict = submission_format[, .N, by=.(prediction_window,series_id)][, .(prediction_window,series_id)]
prediction_window_dict = setNames(prediction_window_dict$prediction_window, prediction_window_dict$series_id)

#### functions ####

merge_metadata = function(dt) {
  merge(dt, meta, on = "series_id", all.x = TRUE)
}

add_is_day_off = function(dt) {
  newDT = copy(dt)
  weekday_cols = newDT[, (weekday_dict[as.character(wday(timestamp))])]
  for (ind in seq_along(weekday_cols)) {
    # set(newDT, ind, "is_day_off", value = eval(parse(text = weekday_cols[ind])))
    newDT[ind, is_day_off := eval(parse(text = weekday_cols[ind]))]
  }
  newDT[, is_day_off := as.integer(is_day_off)]
  newDT
}

drop_week_cols = function(dt) {
  week_cols = c("monday_is_day_off", "tuesday_is_day_off", "wednesday_is_day_off", 
                "thursday_is_day_off", "friday_is_day_off", "saturday_is_day_off", 
                "sunday_is_day_off")
  dt[, !week_cols, with=FALSE]
}


one_hot_encode = function(dt, cols) {
  setDT(dt)
  newDT = copy(dt)
  newDT[, oheID := .I]
  melted = melt(newDT, measure.vars = cols)
  
  encoded_cols = dcast(melted, oheID~variable+value, fun.aggregate = length)
  
  newDT = merge(newDT[, !cols, with=FALSE], encoded_cols, by="oheID")
  newDT[, !"oheID"]
}

add_lagged_feat = function(dt, feature, lag) {
  newDT = copy(dt)
  newDT = newDT[order(series_id, as.POSIXct(timestamp))]
  for(i in seq_len(lag)) {
    new_feature = paste0(feature, "_", i)
    newDT[, (new_feature) := shift(.SD, i), by=series_id, .SDcols=feature]
  }
  newDT
}

impute_temperature = function(dt) {
  # cols = grep("temperature", colnames(dt), value = TRUE)
  temp = dt[, .(series_id, hour = as.POSIXlt(timestamp, tz="UTC")$hour, temperature)][
    , .(avg_temp_per_series = mean(temperature, na.rm = TRUE)), by = .(series_id, hour)][
      , .(series_id, avg_temp_per_series, avg_temp = mean(avg_temp_per_series, na.rm = TRUE)), by = hour
      ]
  
  newDT = copy(dt)
  newDT[, hour := as.POSIXlt(timestamp, tz="UTC")$hour]
  newDT = merge(newDT, temp, by = c("series_id", "hour"), all.x = TRUE)
  newDT[, temperature := dplyr::coalesce(temperature, avg_temp_per_series, avg_temp)]
  newDT[, colnames(dt), with=FALSE]
}

unfold_submission_format = function(dt) {
  newDT = copy(dt)
  newDT[, timestamp := as.POSIXct(timestamp, tz = "UTC")]
  newDT1 = newDT[prediction_window == "hourly"]
  dt_weekly = newDT[prediction_window == "weekly"]
  dt_daily = newDT[prediction_window == "daily"]
  
  list_dt_weekly = split(dt_weekly, 1:nrow(dt_weekly))
  list_dt_weekly = lapply(list_dt_weekly, function(x) x[, .(pred_id, series_id, timestamp = timestamp + c(-144:23) * 3600, temperature, consumption, prediction_window)])
  
  list_dt_daily = split(dt_daily, 1:nrow(dt_daily))
  list_dt_daily = lapply(list_dt_daily, function(x) x[, .(pred_id, series_id, timestamp = timestamp + 3600*0:23, temperature, consumption, prediction_window)])
  
  rbind(newDT1, 
        rbindlist(list_dt_weekly),
        rbindlist(list_dt_daily)
  )
}


predict_iteratively_series = function(series_data, model) {
  dt = copy(series_data)
  dt = dt[order(as.POSIXct(timestamp, "UTC"))]
  N = nrow(dt)
  cols_do_away = c("series_id", "timestamp", "consumption")
  for(it in seq_len(N)) {
    current_row = dt[it]
    current_row = xgb.DMatrix(as.matrix(current_row[, !cols_do_away, with=FALSE]), label = current_row$consumption)
    new_val = predict(model, newdata = current_row)
    set(dt, i=it, j = "consumption", new_val)
    for(k in seq_len(min(24,N-it))) {
      set(dt, i=it+k, j=paste0("consumption_", k), new_val)
    }
  }
  dt
}


aggregate_preds = function(dt) {
  tmp = dt[, .(series_id, timestamp = as.POSIXct(timestamp, "UTC"), consumption)]
  prediction_window = prediction_window_dict[tmp[1, as.character(series_id)]]
  if(prediction_window == "daily") {
    tmp[, grp := ceiling(.I/24)]
    res = tmp[, .(timestamp = format(as.Date(max(timestamp)), "%Y-%m-%d %H:%M:%S"),
                  consumption = sum(consumption),
                  series_id = max(series_id)
    ), by=grp][, .(series_id, timestamp, consumption)]
  }
  if(prediction_window == "weekly") {
    tmp[, grp := ceiling(.I/168)]
    res = tmp[, .(timestamp = format(as.Date(max(timestamp)), "%Y-%m-%d %H:%M:%S"),
                  consumption = sum(consumption),
                  series_id = max(series_id)
    ), by=grp][, .(series_id, timestamp, consumption)]
  }
  if(prediction_window == "hourly") {
    res = dt[, .(series_id, timestamp, consumption)]
  }
  res
}

scale_consumption = function(dt) {
  newDT = copy(dt)
  min_max = newDT[consumption != 0, .(min_value = min(consumption), max_value = max(consumption)), by=series_id]
  newDT = merge(newDT, min_max, by = "series_id")
  newDT[consumption != 0, consumption := (consumption - min_value)/(max_value - min_value + .Machine$double.eps)]
  newDT[, !c("min_value", "max_value")]
}

scale_consumption_return_min_max = function(dt) {
  newDT = copy(dt)
  min_max = newDT[consumption != 0, .(min_value = min(consumption), max_value = max(consumption)), by=series_id]
  min_max
}

#### processing ####
system.time({
  consumption_train_processed_temp_imputed = consumption_train %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 24) %>%
    impute_temperature %>%
    add_lagged_feat("temperature", 3)
})


submission_format_unfolded = unfold_submission_format(submission_format)

system.time({
cold_start_plus_submission = rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]) %>%
  merge_metadata %>%
  add_is_day_off %>%
  drop_week_cols %>%
  one_hot_encode(c("surface", "base_temperature")) %>%
  add_lagged_feat("consumption", 24) %>%
  impute_temperature %>%
  add_lagged_feat("temperature", 3)
})


#### modelling ####
library(xgboost)

train_dt = rbind(consumption_train_processed_temp_imputed, cold_start_plus_submission[consumption>0]) %>% na.omit
num_rows_train = train_dt[, .N]

set.seed(123)
ind_test = sample.int(num_rows_train, size = 0.25*num_rows_train)

test_dt = train_dt[ind_test, !c("series_id", "timestamp")]
train_dt = train_dt[!ind_test, !c("series_id", "timestamp")]

xgb_dtrain = xgb.DMatrix(as.matrix(train_dt[, !"consumption"]), label = train_dt[, consumption])
xgb_dtest = xgb.DMatrix(as.matrix(test_dt[, !"consumption"]), label = test_dt[, consumption])

params1 = list(eta = 0.1, max_depth = 6, colsample_bytree = 0.8)
xgb1 = xgb.train(params = params1, 
                 data = xgb_dtrain,
                 objective = "reg:linear",
                 nrounds = 100,
                 watchlist = list(train = xgb_dtrain, test = xgb_dtest),
                 print_every_n = 10,
                 early_stopping_rounds = 25)


params2 = list(eta = 0.05, max_depth = 6, colsample_bytree = 0.8)
xgb2 = xgb.train(params = params1, 
                 data = xgb_dtrain,
                 objective = "reg:linear",
                 nrounds = 1000,
                 watchlist = list(train = xgb_dtrain, test = xgb_dtest),
                 print_every_n = 20,
                 early_stopping_rounds = 50)
# [1000]	train-rmse:16731.740234	test-rmse:25231.402344 
xgb.save(xgb2, fname = "model_xgb2")

params3 = list(eta = 0.03, max_depth = 6, colsample_bytree = 0.7, subsample = 0.7)
xgb3 = xgb.train(params = params1, 
                 data = xgb_dtrain,
                 objective = "reg:linear",
                 nrounds = 5000,
                 watchlist = list(train = xgb_dtrain, test = xgb_dtest),
                 print_every_n = 100,
                 early_stopping_rounds = 50)
# [2780]	train-rmse:11845.759766	test-rmse:24682.753906

pred_test = cold_start_plus_submission[series_id == 100004 & consumption == 0]
pred_test = pred_test[1]
pred_test = xgb.DMatrix(as.matrix(pred_test[, !c("series_id", "timestamp", "consumption")]), label = pred_test$consumption)
new_val = predict(xgb1, newdata = pred_test)

for(i in 1:336) {
  current_row = pred_test[i]
  current_row = xgb.DMatrix(as.matrix(current_row[, !c("series_id", "timestamp", "consumption")]), label = current_row$consumption)
  new_val = predict(xgb1, newdata = current_row)
  set(pred_test, i=i, j = "consumption", new_val)
  for (k in seq_len(min(24,336-i))) {
    set(pred_test, i=i+k, j=paste0("consumption_", k), new_val)
  }
}

#### prediction ####

data_for_pred = cold_start_plus_submission[consumption == 0][series_id %in% c(100028, 100049, 100054)]

predict_iteratively_series = function(series_data, model) {
  dt = copy(series_data)
  dt = dt[order(as.POSIXct(timestamp, "UTC"))]
  N = nrow(dt)
  for(it in seq_len(N)) {
    current_row = dt[it]
    current_row = xgb.DMatrix(as.matrix(current_row[, model$feature_names, with=FALSE]), label = current_row$consumption)
    new_val = predict(model, newdata = current_row)
    set(dt, i=it, j = "consumption", new_val)
    for(k in seq_len(min(24,N-it))) {
      set(dt, i=it+k, j=paste0("consumption_", k), new_val)
    }
  }
  dt
}

data_for_pred = cold_start_plus_submission[consumption == 0] #[series_id %in% c(100028, 100049, 100054, 100004, 103042, 102146)]
data_for_pred = merge(data_for_pred, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp))], by=c("series_id", "timestamp"))

data_for_pred_list = split(data_for_pred, by = "series_id")

# test = data_for_pred_list[[1]] %>% predict_iteratively_series(model = xgb3)

# test = lapply(data_for_pred_list, function(x) predict_iteratively_series(x, model = xgb3))


### prediction for xgb3
system.time({
  result = lapply(data_for_pred_list, function(x) predict_iteratively_series(x, model = xgb3))
})

prediction_window_dict = submission_format[, .N, by=.(prediction_window,series_id)][, .(prediction_window,series_id)]
prediction_window_dict = setNames(prediction_window_dict$prediction_window, prediction_window_dict$series_id)

aggregated_result = lapply(result, aggregate_preds)

aggregated_result_dt = rbindlist(aggregated_result)

submission_1 = merge(submission_format, aggregated_result_dt[, .(series_id, timestamp, new_consumption = consumption)], by = c("series_id", "timestamp"))
submission_1[, consumption := new_consumption][, new_consumption := NULL]

fwrite(submission_1[order(pred_id), .(pred_id, series_id, timestamp, temperature, consumption, prediction_window)], 
       "submission_1.csv")




#### model bez temperatury ####

meta = fread("meta.csv")

cold_start_test = fread("cold_start_test.csv")
cold_start_test$V1 = NULL
consumption_train = fread("consumption_train.csv")
consumption_train$V1 = NULL

submission_format = fread("submission_format.csv")
weekday_dict = setNames(colnames(meta)[4:10], c(2:7, 1))

### data
system.time({
  consumption_train_processed = consumption_train %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 24)
})
consumption_train_processed$temperature = NULL

submission_format_unfolded = unfold_submission_format(submission_format)

system.time({
  cold_start_plus_submission = rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]) %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 24)
})
cold_start_plus_submission$temperature = NULL

#### modelling ####
library(xgboost)

train_dt = rbind(consumption_train_processed, cold_start_plus_submission[consumption>0]) %>% na.omit
num_rows_train = train_dt[, .N]

set.seed(123)
ind_test = sample.int(num_rows_train, size = 0.25*num_rows_train)

test_dt = train_dt[ind_test, !c("series_id", "timestamp")]
train_dt = train_dt[!ind_test, !c("series_id", "timestamp")]

xgb_dtrain = xgb.DMatrix(as.matrix(train_dt[, !"consumption"]), label = train_dt[, consumption])
xgb_dtest = xgb.DMatrix(as.matrix(test_dt[, !"consumption"]), label = test_dt[, consumption])

params1 = list(eta = 0.05, max_depth = 6, colsample_bytree = 0.8)
xgb_s2_1 = xgb.train(params = params1, 
                 data = xgb_dtrain,
                 objective = "reg:linear",
                 nrounds = 2000,
                 watchlist = list(train = xgb_dtrain, test = xgb_dtest),
                 print_every_n = 20,
                 early_stopping_rounds = 50)

#### prediction ####

data_for_pred = cold_start_plus_submission[consumption == 0] #[series_id %in% c(100028, 100049, 100054, 100004, 103042, 102146)]
data_for_pred = merge(data_for_pred, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp))], by=c("series_id", "timestamp"))

data_for_pred_list = split(data_for_pred, by = "series_id")

system.time({
  result = lapply(data_for_pred_list, function(x) predict_iteratively_series(x, model = xgb_s2_1))
})


aggregated_result = lapply(result, aggregate_preds)

aggregated_result_dt = rbindlist(aggregated_result)

submission_2 = merge(submission_format, aggregated_result_dt[, .(series_id, timestamp, new_consumption = consumption)], by = c("series_id", "timestamp"))
submission_2[, consumption := new_consumption][, new_consumption := NULL]



fwrite(submission_2[order(pred_id), .(pred_id, series_id, timestamp, temperature, consumption, prediction_window)], 
       "submission_2.csv")

####################################################
#### 24h & 48h ####

submission_format_unfolded = unfold_submission_format(submission_format)

### data 24
system.time({
  consumption_train_processed_24 = consumption_train %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 24)
})
consumption_train_processed_24$temperature = NULL


system.time({
  cold_start_plus_submission_24 = rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]) %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 24)
})
cold_start_plus_submission_24$temperature = NULL

### data 48
system.time({
  consumption_train_processed_48 = consumption_train %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 48)
})
consumption_train_processed_48$temperature = NULL

system.time({
  cold_start_plus_submission_48 = rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]) %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 48)
})
cold_start_plus_submission_48$temperature = NULL

#### modelling ####
library(xgboost)

train_dt = rbind(consumption_train_processed_24, cold_start_plus_submission_24[consumption>0]) %>% na.omit
num_rows_train = train_dt[, .N]

set.seed(123)
ind_test = sample.int(num_rows_train, size = 0.25*num_rows_train)

test_dt = train_dt[ind_test, !c("series_id", "timestamp")]
train_dt = train_dt[!ind_test, !c("series_id", "timestamp")]

xgb_dtrain_24 = xgb.DMatrix(as.matrix(train_dt[, !"consumption"]), label = train_dt[, consumption])
xgb_dtest_24 = xgb.DMatrix(as.matrix(test_dt[, !"consumption"]), label = test_dt[, consumption])

train_dt = rbind(consumption_train_processed_48, cold_start_plus_submission_48[consumption>0]) %>% na.omit
num_rows_train = train_dt[, .N]

set.seed(123)
ind_test = sample.int(num_rows_train, size = 0.25*num_rows_train)

test_dt = train_dt[ind_test, !c("series_id", "timestamp")]
train_dt = train_dt[!ind_test, !c("series_id", "timestamp")]

xgb_dtrain_48 = xgb.DMatrix(as.matrix(train_dt[, !"consumption"]), label = train_dt[, consumption])
xgb_dtest_48 = xgb.DMatrix(as.matrix(test_dt[, !"consumption"]), label = test_dt[, consumption])


params1 = list(eta = 0.05, max_depth = 6, colsample_bytree = 0.8)

xgb_s2_24 = xgb.train(params = params1, 
                     data = xgb_dtrain_24,
                     objective = "reg:linear",
                     nrounds = 4000,
                     watchlist = list(train = xgb_dtrain_24, test = xgb_dtest_24),
                     print_every_n = 20,
                     early_stopping_rounds = 50)
xgb.save(xgb_s2_24, fname = "xgb_s2_24")
xgb_s2_24 = xgb.load("xgb_s2_24")

xgb_s2_48 = xgb.train(params = params1, 
                      data = xgb_dtrain_48,
                      objective = "reg:linear",
                      nrounds = 4000,
                      watchlist = list(train = xgb_dtrain_48, test = xgb_dtest_48),
                      print_every_n = 20,
                      early_stopping_rounds = 50)
xgb.save(xgb_s2_48, fname = "xgb_s2_48")
xgb_s2_48 = xgb.load("xgb_s2_48")

#### prediction ####

data_for_pred = cold_start_plus_submission_24[consumption == 0] #[series_id %in% c(100028, 100049, 100054, 100004, 103042, 102146)]
data_for_pred = merge(data_for_pred, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp))], by=c("series_id", "timestamp"))

data_for_pred_list = split(data_for_pred, by = "series_id")

data_for_pred_48 = cold_start_plus_submission_48[consumption == 0] #[series_id %in% c(100028, 100049, 100054, 100004, 103042, 102146)]
data_for_pred_48 = merge(data_for_pred_48, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp))], by=c("series_id", "timestamp"))

data_for_pred_list_48 = split(data_for_pred_48, by = "series_id")

short_series = cold_start_test[, .N, by=series_id][N == 24, series_id]
series_list = submission_format[, unique(series_id)]

system.time({
  result = lapply(series_list, function(x) {
    if(x %in% short_series) predict_iteratively_series(data_for_pred_list[[as.character(x)]], model = xgb_s2_24) else
      predict_iteratively_series(data_for_pred_list_48[[as.character(x)]], model = xgb_s2_48)
  })
})

aggregated_result = lapply(result, aggregate_preds)

aggregated_result_dt = rbindlist(aggregated_result)

submission_3 = merge(submission_format, aggregated_result_dt[, .(series_id, timestamp, new_consumption = consumption)], by = c("series_id", "timestamp"))
submission_3[, consumption := new_consumption][, new_consumption := NULL]

fwrite(submission_3[order(pred_id), .(pred_id, series_id, timestamp, temperature, consumption, prediction_window)], 
       "submission_3.csv")

#### porównanie submission 2 & 3 ####
# submission_2 = fread("submission_2.csv")

merge(submission_2[, .(pred_id, consumption)], submission_3[, .(pred_id, consumption, prediction_window, series_id)], by = "pred_id") %>% View

str(data_for_pred[series_id==102781])
View(data_for_pred_48[series_id==102781])

current_row = xgb.DMatrix(as.matrix(data_for_pred_48[series_id==102781][1, !c("series_id", "timestamp", "consumption")]))
predict(xgb_s2_48, newdata = current_row)                          
current_row = xgb.DMatrix(as.matrix(data_for_pred[series_id==102781][1, !c("series_id", "timestamp", "consumption")]), label = data_for_pred[series_id==102781][1, consumption])
predict(xgb_s2_24, newdata = current_row)  


saveRDS(result, "result_submission3.rds")
# case
result[[1]][, consumption] %>% plot
result[[1]][1, 61:14] %>% unlist %>% plot

##################
#### 24 & 48 consumption + is_day_off, z oryginalną temp, ####

### data
submission_format_unfolded = unfold_submission_format(submission_format)

### data 24
system.time({
  consumption_train_processed_24 = consumption_train %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 24) %>%
    add_lagged_feat("is_day_off", 24)
})

system.time({
  cold_start_plus_submission_24 = rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]) %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 24) %>%
    add_lagged_feat("is_day_off", 24)
})


### data 48
system.time({
  consumption_train_processed_48 = consumption_train %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 48) %>%
    add_lagged_feat("is_day_off", 48)
})

system.time({
  cold_start_plus_submission_48 = rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]) %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    add_lagged_feat("consumption", 48) %>%
    add_lagged_feat("is_day_off", 48)
})

#### modelling ####
library(xgboost)

train_dt = rbind(consumption_train_processed_24, cold_start_plus_submission_24[consumption>0]) %>% na.omit
num_rows_train = train_dt[, .N]

set.seed(123)
ind_test = sample.int(num_rows_train, size = 0.25*num_rows_train)

test_dt = train_dt[ind_test, !c("series_id", "timestamp")]
train_dt = train_dt[!ind_test, !c("series_id", "timestamp")]

xgb_dtrain_24 = xgb.DMatrix(as.matrix(train_dt[, !"consumption"]), label = train_dt[, consumption])
xgb_dtest_24 = xgb.DMatrix(as.matrix(test_dt[, !"consumption"]), label = test_dt[, consumption])

train_dt = rbind(consumption_train_processed_48, cold_start_plus_submission_48[consumption>0]) %>% na.omit
num_rows_train = train_dt[, .N]

set.seed(123)
ind_test = sample.int(num_rows_train, size = 0.25*num_rows_train)

test_dt = train_dt[ind_test, !c("series_id", "timestamp")]
train_dt = train_dt[!ind_test, !c("series_id", "timestamp")]

xgb_dtrain_48 = xgb.DMatrix(as.matrix(train_dt[, !"consumption"]), label = train_dt[, consumption])
xgb_dtest_48 = xgb.DMatrix(as.matrix(test_dt[, !"consumption"]), label = test_dt[, consumption])


params1 = list(eta = 0.1, max_depth = 6, colsample_bytree = 0.8)

xgb_s4_24 = xgb.train(params = params1, 
                      data = xgb_dtrain_24,
                      objective = "reg:linear",
                      nrounds = 1500,
                      watchlist = list(train = xgb_dtrain_24, test = xgb_dtest_24),
                      print_every_n = 20,
                      early_stopping_rounds = 50)
xgb.save(xgb_s4_24, fname = "xgb_s4_24")
# xgb_s4_24 = xgb.load("xgb_s4_24")

xgb_s4_48 = xgb.train(params = params1, 
                      data = xgb_dtrain_48,
                      objective = "reg:linear",
                      nrounds = 1500,
                      watchlist = list(train = xgb_dtrain_48, test = xgb_dtest_48),
                      print_every_n = 20,
                      early_stopping_rounds = 50)
xgb.save(xgb_s4_48, fname = "xgb_s4_48")
# xgb_s4_48 = xgb.load("xgb_s4_48")

#### prediction ####

data_for_pred = cold_start_plus_submission_24[consumption == 0] #[series_id %in% c(100028, 100049, 100054, 100004, 103042, 102146)]
data_for_pred = merge(data_for_pred, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp))], by=c("series_id", "timestamp"))

data_for_pred_list = split(data_for_pred, by = "series_id")

data_for_pred_48 = cold_start_plus_submission_48[consumption == 0] #[series_id %in% c(100028, 100049, 100054, 100004, 103042, 102146)]
data_for_pred_48 = merge(data_for_pred_48, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp))], by=c("series_id", "timestamp"))

data_for_pred_list_48 = split(data_for_pred_48, by = "series_id")

short_series = cold_start_test[, .N, by=series_id][N == 24, series_id]
series_list = submission_format[, unique(series_id)]

system.time({
  result = lapply(series_list, function(x) {
    if(x %in% short_series) predict_iteratively_series(data_for_pred_list[[as.character(x)]], model = xgb_s4_24) else
      predict_iteratively_series(data_for_pred_list_48[[as.character(x)]], model = xgb_s4_48)
  })
})
saveRDS(result, "result_submission_4.rds")

aggregated_result = lapply(result, aggregate_preds)

aggregated_result_dt = rbindlist(aggregated_result)

submission_4 = merge(submission_format, aggregated_result_dt[, .(series_id, timestamp, new_consumption = consumption)], by = c("series_id", "timestamp"))
submission_4[, consumption := new_consumption][, new_consumption := NULL]

fwrite(submission_4[order(pred_id), .(pred_id, series_id, timestamp, temperature, consumption, prediction_window)], 
       "submission_4.csv")

# sprawdzenie
result[[1]][, consumption] %>% plot
result[[1]][1, 61:14] %>% unlist %>% plot


#### jak poprzednie , ale wyskalowany target ####
### data
submission_format_unfolded = unfold_submission_format(submission_format)

### data 24
system.time({
  consumption_train_processed_24 = consumption_train %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    scale_consumption %>%
    add_lagged_feat("consumption", 24) %>%
    add_lagged_feat("is_day_off", 24)
})

system.time({
  cold_start_plus_submission_24 = rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]) %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    scale_consumption %>%
    add_lagged_feat("consumption", 24) %>%
    add_lagged_feat("is_day_off", 24)
})


### data 48
system.time({
  consumption_train_processed_48 = consumption_train %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    scale_consumption %>%
    add_lagged_feat("consumption", 48) %>%
    add_lagged_feat("is_day_off", 48)
})

system.time({
  cold_start_plus_submission_48 = rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]) %>%
    merge_metadata %>%
    add_is_day_off %>%
    drop_week_cols %>%
    one_hot_encode(c("surface", "base_temperature")) %>%
    scale_consumption %>%
    add_lagged_feat("consumption", 48) %>%
    add_lagged_feat("is_day_off", 48)
})

dt_scaler_min_max_cold_start_subm = scale_consumption_return_min_max(rbind(cold_start_test, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp), consumption, temperature)]))

#### modelling ####
library(xgboost)

train_dt = consumption_train_processed_24 %>% na.omit
num_rows_train = train_dt[, .N]

set.seed(123)
ind_test = sample.int(num_rows_train, size = 0.25*num_rows_train)

test_dt = train_dt[ind_test, !c("series_id", "timestamp")]
train_dt = train_dt[!ind_test, !c("series_id", "timestamp")]

xgb_dtrain_24 = xgb.DMatrix(as.matrix(train_dt[, !"consumption"]), label = train_dt[, consumption])
xgb_dtest_24 = xgb.DMatrix(as.matrix(test_dt[, !"consumption"]), label = test_dt[, consumption])

train_dt = consumption_train_processed_48 %>% na.omit
num_rows_train = train_dt[, .N]

set.seed(123)
ind_test = sample.int(num_rows_train, size = 0.25*num_rows_train)

test_dt = train_dt[ind_test, !c("series_id", "timestamp")]
train_dt = train_dt[!ind_test, !c("series_id", "timestamp")]

xgb_dtrain_48 = xgb.DMatrix(as.matrix(train_dt[, !"consumption"]), label = train_dt[, consumption])
xgb_dtest_48 = xgb.DMatrix(as.matrix(test_dt[, !"consumption"]), label = test_dt[, consumption])


params1 = list(eta = 0.15, max_depth = 6, colsample_bytree = 0.8)

xgb_s5_24 = xgb.train(params = params1, 
                      data = xgb_dtrain_24,
                      objective = "reg:logistic",
                      nrounds = 1500,
                      watchlist = list(train = xgb_dtrain_24, test = xgb_dtest_24),
                      print_every_n = 20,
                      early_stopping_rounds = 50)
xgb.save(xgb_s5_24, fname = "xgb_s5_24")
# xgb_s5_24 = xgb.load("xgb_s5_24")

xgb_s5_48 = xgb.train(params = params1, 
                      data = xgb_dtrain_48,
                      objective = "reg:logistic",
                      nrounds = 1500,
                      watchlist = list(train = xgb_dtrain_48, test = xgb_dtest_48),
                      print_every_n = 20,
                      early_stopping_rounds = 50)
xgb.save(xgb_s5_48, fname = "xgb_s5_48")
# xgb_s5_48 = xgb.load("xgb_s5_48")



#### prediction ####

data_for_pred = cold_start_plus_submission_24[consumption == 0] #[series_id %in% c(100028, 100049, 100054, 100004, 103042, 102146)]
data_for_pred = merge(data_for_pred, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp))], by=c("series_id", "timestamp"))

data_for_pred_list = split(data_for_pred, by = "series_id")

data_for_pred_48 = cold_start_plus_submission_48[consumption == 0] #[series_id %in% c(100028, 100049, 100054, 100004, 103042, 102146)]
data_for_pred_48 = merge(data_for_pred_48, submission_format_unfolded[, .(series_id, timestamp = as.character(timestamp))], by=c("series_id", "timestamp"))

data_for_pred_list_48 = split(data_for_pred_48, by = "series_id")

short_series = cold_start_test[, .N, by=series_id][N == 24, series_id]
series_list = submission_format[, unique(series_id)]

system.time({
  result = lapply(series_list, function(x) {
    if(x %in% short_series) predict_iteratively_series(data_for_pred_list[[as.character(x)]], model = xgb_s5_24) else
      predict_iteratively_series(data_for_pred_list_48[[as.character(x)]], model = xgb_s5_48)
  })
})
saveRDS(result, "result_submission_5.rds")

### przekształcić z powrotem
reverse_scaler_consumption = function(dt) {
  newDT = merge(dt, dt_scaler_min_max_cold_start_subm, by = "series_id")
  newDT[, new_consumption := (max_value-min_value)*consumption + min_value]
  newDT[, consumption := new_consumption]
  newDT
}

reverse_scaled_result = lapply(result, reverse_scaler_consumption)

aggregated_result = lapply(reverse_scaled_result, aggregate_preds)

aggregated_result_dt = rbindlist(aggregated_result)

submission_5 = merge(submission_format, aggregated_result_dt[, .(series_id, timestamp, new_consumption = consumption)], by = c("series_id", "timestamp"))
submission_5[, consumption := new_consumption][, new_consumption := NULL]

fwrite(submission_5[order(pred_id), .(pred_id, series_id, timestamp, temperature, consumption, prediction_window)], 
       "submission_5.csv")
