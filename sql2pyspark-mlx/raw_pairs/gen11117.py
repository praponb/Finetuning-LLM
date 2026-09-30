from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("device_code")
    .agg(
        F.sum("pressure").alias("total_pressure"),
        F.sum(F.when(F.col("device_code") == "DV-04", F.col("pressure")).otherwise(0)).alias("dv_04_pressure"),
        F.stddev_samp("status_bits").alias("status_bits_stddev"),
    )
    .orderBy("device_code")
)
