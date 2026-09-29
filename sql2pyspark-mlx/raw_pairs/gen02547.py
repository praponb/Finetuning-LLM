from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("event_type").orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.orderBy(F.col("running_count_duration_sec_filled").desc(), "id")
w4 = Window.orderBy("duration_sec", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy(F.col("running_running_avg_duration_sec_filled").desc(), "id")
w6 = Window.orderBy("running_sum_duration_sec_filled_avg_2", "id").rowsBetween(-3, -1)

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-06-21"))) & F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.col("event_time").cast("date").alias("event_time_day"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
    )
)

scored = (
    cleaned
    .filter(F.col("event_time_day") > "2023-06-15")
    .select(
        "id",
        "event_time",
        "duration_sec",
        F.count("duration_sec_filled").over(w_part).alias("running_count_duration_sec_filled"),
        F.min("duration_sec_filled").over(w_part).alias("running_min_duration_sec_filled"),
        F.avg("duration_sec_filled").over(w_part).alias("running_avg_duration_sec_filled"),
        F.sum("duration_sec_filled").over(w_part).alias("running_sum_duration_sec_filled"),
    )
)

with_windows = (
    scored
    .withColumn("running_running_avg_duration_sec_filled", F.sum("running_avg_duration_sec_filled").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist_2))
    .withColumn("running_sum_duration_sec_filled_avg_2", F.avg("running_sum_duration_sec_filled").over(w4))
)

with_windows_web_events = (
    with_windows
    .select(
        "id",
        "event_time",
        "running_count_duration_sec_filled",
        "running_avg_duration_sec_filled",
        "running_sum_duration_sec_filled",
        "running_sum_duration_sec_filled_avg_2",
        F.ntile(10).over(w5).alias("running_running_avg_duration_sec_filled_bucket"),
        F.avg("running_sum_duration_sec_filled_avg_2").over(w6).alias("running_sum_duration_sec_filled_avg_2_avg_3_prior"),
    )
)

result = (
    with_windows_web_events
    .select(
        "id",
        "running_avg_duration_sec_filled",
        "running_sum_duration_sec_filled",
        "running_running_avg_duration_sec_filled_bucket",
        "running_sum_duration_sec_filled_avg_2_avg_3_prior",
    )
    .orderBy(F.col("id").desc_nulls_first())
    .limit(20)
)
