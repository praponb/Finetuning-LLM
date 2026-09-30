from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy(
        F.when(F.col("pressure") >= 1025, "HIGH").otherwise("LOW").alias("pressure_tier"),
        F.when(F.col("status_bits") >= 13, "PREMIUM").otherwise("STANDARD").alias("status_bits_tier"),
    )
    .agg(
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("temp_c"))).alias("dv_04_avg_temp_c"),
        F.corr("status_bits", "pressure").alias("corr_status_bits_pressure"),
    )
)
