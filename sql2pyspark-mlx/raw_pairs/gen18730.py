from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("running_max_burst_id").desc(), "id")
w4 = Window.partitionBy("device_code").orderBy("running_min_burst_id", "id")
w5 = Window.orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 07:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        F.ceil("status_bits").alias("status_bits_whole"),
    )
)

prev_events = cleaned.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("session_no"),
    )
)

with_gaps_2 = (
    sessions_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands = (
    with_gaps_2
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

metrics = (
    islands
    .select(
        "id",
        "device_code",
        "reading_ts",
        F.sum("burst_id").over(w2).alias("running_sum_burst_id"),
        F.count("burst_id").over(w2).alias("running_count_burst_id"),
        F.min("burst_id").over(w2).alias("running_min_burst_id"),
        F.max("burst_id").over(w2).alias("running_max_burst_id"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "running_min_burst_id",
        "running_max_burst_id",
    )
)

scored = (
    ranked
    .select(
        "*",
        F.lag("running_min_burst_id").over(w4).alias("prev_running_min_burst_id"),
        F.lead("running_min_burst_id").over(w4).alias("next_running_min_burst_id"),
        F.lag("running_max_burst_id").over(w4).alias("prev_running_max_burst_id"),
        F.row_number().over(w4).alias("seq_no"),
        F.sum("running_max_burst_id").over(w5).alias("running_running_max_burst_id"),
    )
)

totals = (
    scored
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .rollup("activity_day")
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
        F.grouping("activity_day").alias("gid"),
    )
)

result = (
    totals
    .select("activity_day", "event_count")
    .orderBy(F.col("activity_day").desc(), "event_count")
)
