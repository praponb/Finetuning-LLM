from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        F.ceil("temp_c").alias("temp_c_whole"),
        (F.col("humidity") + F.col("pressure")).alias("humidity_plus_pressure"),
    )
)

cleaned = (
    filtered
    .filter((F.col("reading_ts") > F.lit("2025-09-01 04:00:00").cast("timestamp")) & F.col("humidity_plus_pressure").isNotNull())
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("status_bits") - F.col("temp_c_whole")).alias("status_bits_minus_temp_c_whole"),
    )
)

valid = (
    cleaned
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "*",
        F.greatest("pressure", "status_bits").alias("greatest_pressure_status_bits"),
        F.coalesce("pressure", F.lit(0)).alias("pressure_filled"),
    )
)

valid_device_readings = (
    valid
    .filter(F.col("humidity") <= 69)
    .select(
        "id",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "status_bits_minus_temp_c_whole",
        "greatest_pressure_status_bits",
        "pressure_filled",
        (F.col("pressure") + F.col("status_bits_minus_temp_c_whole")).alias("pressure_plus_status_bits_minus_temp_c_whole"),
    )
)

valid_device_readings_2 = (
    valid_device_readings
    .filter(F.col("temp_c").between(24, 33))
    .withColumn("temp_c_times_humidity", (F.col("temp_c") * F.col("humidity")))
)

metrics = (
    valid_device_readings_2
    .filter(F.col("firmware") == "1.0.3")
    .select(
        "*",
        F.min("status_bits_minus_temp_c_whole").over(w_ordered).alias("running_min_status_bits_minus_temp_c_whole"),
        F.max("status_bits_minus_temp_c_whole").over(w_ordered).alias("running_max_status_bits_minus_temp_c_whole"),
        F.count("status_bits_minus_temp_c_whole").over(w_ordered).alias("running_count_status_bits_minus_temp_c_whole"),
        F.sum("status_bits_minus_temp_c_whole").over(w_ordered).alias("running_sum_status_bits_minus_temp_c_whole"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("humidity") >= 77, "HIGH").otherwise("LOW").alias("humidity_tier"),
    )
    .agg(
        F.round(F.avg("pressure_plus_status_bits_minus_temp_c_whole"), 2).alias("avg_pressure_plus_status_bits_minus_temp_c_whole"),
        F.avg("running_count_status_bits_minus_temp_c_whole").alias("avg_running_count_status_bits_minus_temp_c_whole"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
    )
)
