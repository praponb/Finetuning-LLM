from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.date_trunc("HOUR", "reading_ts").alias("hour_window"),
        F.when(F.col("temp_c") >= 39.2, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
        "device_code",
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.min("temp_c").alias("min_temp_c"),
        F.corr("pressure", "humidity").alias("corr_pressure_humidity"),
    )
    .filter(F.col("corr_pressure_humidity") > 0)
    .orderBy(F.col("temp_c_tier").desc_nulls_last())
)
