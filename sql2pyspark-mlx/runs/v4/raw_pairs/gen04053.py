from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("device_code")
    .agg(
        F.percentile_approx("humidity", [0.25, 0.5, 0.75]).alias("humidity_quartiles"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("status_bits")).otherwise(0)).alias("dv_01_status_bits"),
    )
)
