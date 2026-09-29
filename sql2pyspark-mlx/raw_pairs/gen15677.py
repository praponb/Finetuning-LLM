from pyspark.sql import functions as F

result = (
    df_device_readings
    .rollup(
        "device_code",
        F.when(F.col("status_bits") >= 10, "HIGH").otherwise("LOW").alias("status_bits_tier"),
        F.date_trunc("DAY", "reading_ts").alias("activity_day"),
    )
    .agg(
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.min("reading_ts").alias("started_at"),
        F.percentile_approx("temp_c", [0.25, 0.5, 0.75]).alias("temp_c_quartiles"),
    )
    .orderBy("status_bits_tier")
)
