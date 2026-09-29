from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("status_bits") >= 11, "LARGE").otherwise("SMALL").alias("status_bits_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
        F.min("reading_ts").alias("first_reading_ts"),
        F.corr("humidity", "temp_c").alias("corr_humidity_temp_c"),
    )
    .orderBy("status_bits_tier")
)
