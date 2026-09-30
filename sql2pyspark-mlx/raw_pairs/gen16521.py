from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts_day_period", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts_day_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts_date").desc(), "id")
w4 = Window.partitionBy(F.month("reading_ts_day_period"), "device_code").orderBy("weeks_since_reading_ts_day_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("reading_ts_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("weeks_since_reading_ts_day_period", "id")

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        F.to_date("reading_ts").alias("reading_ts_date"),
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
    )
)

valid = (
    filtered
    .filter(F.col("temp_c") > 37)
    .withColumn("reading_ts_day_period", F.date_trunc("year", "reading_ts_day"))
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("reading_ts_day_period") - F.lag("reading_ts_day_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "reading_ts_date",
        "reading_ts_day_period",
        F.sum("session_start_flag").over(w2).alias("island_id"),
    )
)

filtered_device_readings = (
    islands
    .filter(F.to_date("reading_ts_day_period") >= "2025-08-01")
    .select(
        "*",
        F.date_format("reading_ts_date", "MMM yyyy").alias("reading_ts_date_label"),
        (F.datediff(F.current_date(), "reading_ts_day_period") / 7).alias("weeks_since_reading_ts_day_period"),
    )
)

latest = (
    filtered_device_readings
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    latest
    .select(
        "id",
        "pressure",
        "reading_ts_date",
        "reading_ts_day_period",
        "island_id",
        "reading_ts_date_label",
        F.sum("island_id").over(w4).alias("running_island_id"),
        F.sum("weeks_since_reading_ts_day_period").over(w5).alias("running_weeks_since_reading_ts_day_period"),
        F.ntile(10).over(w6).alias("weeks_since_reading_ts_day_period_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "pressure",
        "island_id",
        "reading_ts_date_label",
        "running_island_id",
        "running_weeks_since_reading_ts_day_period",
        "weeks_since_reading_ts_day_period_bucket",
    )
    .orderBy("pressure")
)
