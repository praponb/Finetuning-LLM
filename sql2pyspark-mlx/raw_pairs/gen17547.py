from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("island_id").desc(), "id")
w_grp = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_rn_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("pressure").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select("*", F.round("humidity", 2).alias("humidity_rounded"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

prev_events = deduped.withColumn("prev_reading_ts", F.lag("reading_ts").over(w2))

flagged = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "humidity_rounded",
        "rn",
        F.sum("session_start_flag").over(w3).alias("island_id"),
    )
)

deduped_device_readings = (
    sessions_device_readings
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "device_code", "pressure", "firmware", "rn", "island_id")
)

base = (
    deduped_device_readings
    .filter(F.col("pressure") > 1022)
    .select(
        "id",
        "pressure",
        "firmware",
        "rn",
        "island_id",
        F.round("rn", 2).alias("rn_rounded"),
    )
)

scored = (
    base
    .filter(F.col("firmware").isin(["2.0.1", "1.0.3", "1.1.0"]))
    .select(
        "id",
        "pressure",
        "rn",
        F.sum("rn_rounded").over(w_grp).alias("cumulative_rn_rounded"),
        F.sum("rn_rounded").over(w_grp).alias("running_rn_rounded"),
    )
)

with_windows = (
    scored
    .withColumn("cumulative_cumulative_rn_rounded", F.sum("cumulative_rn_rounded").over(w6))
    .withColumn("running_cumulative_rn_rounded", F.sum("cumulative_rn_rounded").over(w7))
    .withColumn("pressure_bucket", F.ntile(4).over(w8))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("cumulative_rn_rounded") >= 415, "HIGH").otherwise("LOW").alias("cumulative_rn_rounded_tier"),
        F.when(F.col("cumulative_cumulative_rn_rounded") >= 2025, "LARGE").otherwise("SMALL").alias("cumulative_cumulative_rn_rounded_tier"),
        F.when(F.col("running_rn_rounded") >= 323, "PREMIUM").otherwise("STANDARD").alias("running_rn_rounded_tier"),
    )
    .agg(
        F.round(F.avg("running_rn_rounded"), 2).alias("avg_running_rn_rounded"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_rn_rounded"), 2).alias("avg_running_rn_rounded_2"),
    )
    .orderBy(F.col("avg_running_rn_rounded").asc_nulls_first())
)
