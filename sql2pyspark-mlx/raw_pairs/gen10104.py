from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("island_id", "id")
w4 = Window.orderBy("greatest_pressure_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("pressure").desc(), "id")
w6 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("island_id", "id").rowsBetween(-3, -1)

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

valid = (
    sessions_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.to_date("reading_ts") >= "2025-05-01"))
    .withColumn("island_id_plus_temp_c", (F.col("island_id") + F.col("temp_c")))
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "temp_c", "pressure", "firmware", "island_id")
)

cleaned = (
    latest
    .filter(F.col("firmware") == "1.1.0")
    .select(
        "*",
        F.greatest("pressure", "temp_c").alias("greatest_pressure_temp_c"),
        F.round((F.col("island_id") / F.col("pressure")), 2).alias("island_id_per_pressure"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.last("island_id_per_pressure", ignorenulls=True).over(w4).alias("last_known_island_id_per_pressure"),
        F.ntile(4).over(w5).alias("pressure_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "temp_c",
        "pressure",
        "firmware",
        "island_id",
        "greatest_pressure_temp_c",
        "pressure_bucket",
        F.last("pressure_bucket", ignorenulls=True).over(w6).alias("last_known_pressure_bucket"),
        F.avg("pressure_bucket").over(w7).alias("pressure_bucket_avg_3_prior"),
    )
)

filtered = (
    with_windows
    .filter(F.col("temp_c") <= 25)
    .select(
        "id",
        "temp_c",
        "pressure",
        "firmware",
        "island_id",
        "greatest_pressure_temp_c",
        "pressure_bucket",
        "last_known_pressure_bucket",
        "pressure_bucket_avg_3_prior",
        F.round("last_known_pressure_bucket", 1).alias("last_known_pressure_bucket_rounded"),
    )
)

result = (
    filtered
    .groupBy("island_id")
    .agg(
        F.sum("pressure_bucket_avg_3_prior").alias("total_pressure_bucket_avg_3_prior"),
        F.percentile_approx("last_known_pressure_bucket_rounded", 0.5).alias("last_known_pressure_bucket_rounded_p50"),
    )
    .orderBy(F.col("total_pressure_bucket_avg_3_prior").desc_nulls_last())
)
