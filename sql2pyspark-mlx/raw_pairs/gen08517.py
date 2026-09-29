from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & ~F.col("firmware").isin(["1.0.3", "1.1.0"]))
    .withColumn("temp_c_band", F.when(F.col("temp_c") >= 40.7, "High").when(F.col("temp_c") >= 20.5, "Medium").otherwise("Low"))
    .withColumn("pressure_minus_humidity", (F.col("pressure") - F.col("humidity")))
)

result = (
    filtered
    .groupBy(
        F.when(F.col("pressure_minus_humidity") >= 78, "PREMIUM").otherwise("STANDARD").alias("pressure_minus_humidity_tier"),
    )
    .agg(
        F.round(F.avg("status_bits"), 2).alias("avg_status_bits"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_count"),
        F.avg("humidity").alias("avg_humidity"),
    )
    .filter(F.col("avg_humidity") > 0)
)
