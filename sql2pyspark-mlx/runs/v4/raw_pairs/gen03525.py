from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("humidity_int_minus_pressure_tier").orderBy(F.col("humidity_int_p90").desc())

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= "2025-05-15")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        F.col("humidity").cast("int").alias("humidity_int"),
    )
)

cleaned = (
    valid
    .filter(F.col("reading_ts") > F.to_date(F.lit("2025-09-01")))
    .withColumn("status_bits_plus_pressure", (F.col("status_bits") + F.col("pressure")))
)

filtered = (
    cleaned
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .withColumn("humidity_int_minus_pressure", (F.col("humidity_int") - F.col("pressure")))
)

stats = (
    filtered
    .groupBy(
        F.when(F.col("humidity_int_minus_pressure") >= 71, "HIGH").otherwise("LOW").alias("humidity_int_minus_pressure_tier"),
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
    )
    .agg(
        F.min("reading_ts").alias("first_reading_ts"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.sum("humidity_int_minus_pressure").alias("total_humidity_int_minus_pressure"),
        F.stddev_samp("pressure").alias("pressure_stddev"),
        F.percentile_approx("humidity_int", 0.9).alias("humidity_int_p90"),
    )
    .withColumn("humidity_int_p90_rank", F.rank().over(w))
)

result = (
    stats
    .select(
        "humidity_int_minus_pressure_tier",
        "hour_window",
        "first_reading_ts",
        "pressure_stddev",
    )
)
