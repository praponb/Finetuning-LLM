from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("device_code").orderBy("humidity", "id")
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("reading_ts")
w5 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.year("reading_ts"))
w7 = Window.orderBy(F.col("running_avg_temp_c").desc(), "id")
w8 = Window.orderBy(F.col("prev_humidity").desc(), "id")
w_running = Window.orderBy("running_avg_temp_c_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("cumulative_running_avg_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("prev_humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("last_known_reading_ts_year_sum_status_bits", "id")
w13 = Window.orderBy(F.col("event_seq").desc_nulls_first())

metrics = (
    df_device_readings
    .filter(F.col("status_bits") < 4)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        F.avg("temp_c").over(w_ordered).alias("running_avg_temp_c"),
        F.min("temp_c").over(w_ordered).alias("running_min_temp_c"),
        F.sum("temp_c").over(w_ordered).alias("running_sum_temp_c"),
        F.lag("humidity", 1).over(w_hist).alias("prev_humidity"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    ranked
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    deduped
    .select(
        "id",
        "prev_humidity",
        F.sum("running_avg_temp_c").over(w5).alias("cumulative_running_avg_temp_c"),
        F.sum("status_bits").over(w6).alias("reading_ts_year_sum_status_bits"),
        F.ntile(3).over(w7).alias("running_avg_temp_c_bucket"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "prev_humidity",
        "cumulative_running_avg_temp_c",
        "running_avg_temp_c_bucket",
        F.row_number().over(w8).alias("event_seq"),
        F.last("prev_humidity", ignorenulls=True).over(w_running).alias("last_known_prev_humidity"),
        F.sum("reading_ts_year_sum_status_bits").over(w10).alias("cumulative_reading_ts_year_sum_status_bits"),
        F.last("reading_ts_year_sum_status_bits", ignorenulls=True).over(w_running).alias("last_known_reading_ts_year_sum_status_bits"),
        F.last("cumulative_running_avg_temp_c", ignorenulls=True).over(w11).alias("last_known_cumulative_running_avg_temp_c"),
    )
)

base = (
    scored
    .filter(F.col("last_known_prev_humidity") > 42)
    .withColumn("last_known_cumulative_running_avg_temp_c_whole", F.floor("last_known_cumulative_running_avg_temp_c"))
    .withColumn("last_known_cumulative_running_avg_temp_c_whole_2", F.floor("last_known_cumulative_running_avg_temp_c"))
)

metrics_device_readings = (
    base
    .select(
        "*",
        F.lead("last_known_prev_humidity").over(w12).alias("next_last_known_prev_humidity"),
        F.lead("event_seq").over(w12).alias("next_event_seq"),
        F.lag("event_seq").over(w12).alias("prev_event_seq"),
        F.row_number().over(w12).alias("event_seq_2"),
        F.percent_rank().over(w13).alias("event_seq_percent_rank"),
    )
)

report = (
    metrics_device_readings
    .select(
        "id",
        "prev_humidity",
        "running_avg_temp_c_bucket",
        "event_seq",
        "cumulative_reading_ts_year_sum_status_bits",
        "last_known_reading_ts_year_sum_status_bits",
        "last_known_cumulative_running_avg_temp_c",
        "last_known_cumulative_running_avg_temp_c_whole",
        "next_last_known_prev_humidity",
    )
    .orderBy(F.col("prev_humidity").desc_nulls_first())
)

result = (
    report
    .select(
        "id",
        "prev_humidity",
        "running_avg_temp_c_bucket",
        "event_seq",
        "last_known_reading_ts_year_sum_status_bits",
        "last_known_cumulative_running_avg_temp_c",
        "last_known_cumulative_running_avg_temp_c_whole",
    )
    .orderBy(
        F.col("last_known_reading_ts_year_sum_status_bits").desc(),
        "last_known_cumulative_running_avg_temp_c_whole",
    )
)
