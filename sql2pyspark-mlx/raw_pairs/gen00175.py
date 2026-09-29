from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits_per_pressure", "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 01:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("status_bits") / F.col("pressure")), 2).alias("status_bits_per_pressure"),
        F.greatest("temp_c", "humidity").alias("greatest_temp_c_humidity"),
    )
)

valid = (
    base
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "status_bits_per_pressure",
        "greatest_temp_c_humidity",
        F.least("temp_c", "status_bits").alias("least_temp_c_status_bits"),
    )
)

filtered = (
    valid
    .filter(F.col("status_bits") >= 4)
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "status_bits_per_pressure",
        "greatest_temp_c_humidity",
        "least_temp_c_status_bits",
        F.col("status_bits").cast("double").alias("status_bits_str"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "humidity",
        "status_bits",
        "status_bits_per_pressure",
        "greatest_temp_c_humidity",
        "least_temp_c_status_bits",
        "status_bits_str",
    )
)

filtered_device_readings = (
    ranked
    .filter(F.col("humidity").between(53, 63))
    .select(
        "id",
        "status_bits",
        "greatest_temp_c_humidity",
        "least_temp_c_status_bits",
        F.round("greatest_temp_c_humidity", 1).alias("greatest_temp_c_humidity_rounded"),
    )
)

valid_device_readings = (
    filtered_device_readings
    .filter(F.col("greatest_temp_c_humidity").between(45, 63))
    .select(
        "id",
        "status_bits",
        "greatest_temp_c_humidity",
        "least_temp_c_status_bits",
        "greatest_temp_c_humidity_rounded",
        (F.col("greatest_temp_c_humidity") * F.col("least_temp_c_status_bits")).alias("greatest_temp_c_humidity_times_least_temp_c_status_bits"),
    )
)

result = (
    valid_device_readings
    .groupBy(
        F.when(F.col("greatest_temp_c_humidity_times_least_temp_c_status_bits") >= 87, "LARGE").otherwise("SMALL").alias("greatest_temp_c_humidity_times_least_temp_c_status_bits_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("least_temp_c_status_bits", [0.25, 0.5, 0.75]).alias("least_temp_c_status_bits_quartiles"),
        F.sum("greatest_temp_c_humidity_times_least_temp_c_status_bits").alias("total_greatest_temp_c_humidity_times_least_temp_c_status_bits"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
    )
    .orderBy(F.col("num_rows").desc())
)
