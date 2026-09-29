from pyspark.sql import functions as F

metrics_long = (
    df_device_readings
    .select("id", "device_code", "temp_c", F.col("pressure").alias("PRESSURE_VAL"))
    .unpivot(["id", "device_code"], ["temp_c", "PRESSURE_VAL"], "metric_name", "value")
)

result = (
    metrics_long
    .groupBy("device_code")
    .agg(
        F.count(F.when(F.col("metric_name") == "temp_c", 1)).alias("temp_c_count"),
        F.count(F.when(F.col("metric_name") == "temp_c", 1)).alias("temp_c_rows"),
    )
    .orderBy(F.col("device_code").desc())
)
