from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("burst_id", "id")
w4 = Window.partitionBy("hour_window").orderBy(F.col("distinct_ids").desc())

base = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("status_bits") >= 5))
    .select(
        "*",
        F.hour("reading_ts").alias("reading_ts_hour"),
        F.when(F.col("device_code").isin(["DV-03", "DV-01"]), "Priority").otherwise("Other").alias("device_code_group"),
    )
)

flagged = (
    base
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "status_bits",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

with_windows = (
    sessions_device_readings
    .select(
        "*",
        F.lag("burst_id").over(w3).alias("prev_burst_id"),
        F.lag("status_bits").over(w3).alias("prev_status_bits"),
        F.lead("status_bits").over(w3).alias("next_status_bits"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

valid = (
    with_windows
    .filter(F.col("reading_ts") >= "2024-11-08")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "burst_id",
        "prev_burst_id",
        "event_seq",
        F.length("device_code").alias("device_code_length"),
    )
)

flagged_2 = (
    valid
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

islands = (
    flagged_2
    .select(
        "id",
        "device_code",
        "reading_ts",
        "event_seq",
        "device_code_length",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

daily = (
    islands
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("device_code_length", 0.95).alias("device_code_length_p95"),
    )
    .withColumn("distinct_ids_dense_rank", F.dense_rank().over(w4))
)

result = (
    daily
    .select(
        "hour_window",
        "activity_day",
        "distinct_ids",
        "device_code_length_p95",
        "distinct_ids_dense_rank",
    )
)
