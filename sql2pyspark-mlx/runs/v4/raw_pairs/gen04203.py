from pyspark.sql import functions as F

metrics_long = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("pressure").alias("PRESSURE_VAL"),
        "humidity",
        "temp_c",
    )
    .unpivot(
        ["id", "device_code"],
        ["PRESSURE_VAL", "humidity", "temp_c"],
        "measure",
        "value",
    )
)

result = (
    metrics_long
    .rollup(
        "device_code",
        F.when(F.col("value") >= 83, "HIGH").otherwise("LOW").alias("value_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("value", 0.95).alias("value_p95"),
        F.percentile_approx("value", 0.9).alias("value_p90"),
        F.round(F.avg("value"), 2).alias("avg_value"),
        F.percentile_approx("value", 0.5).alias("value_p50"),
        F.sum(F.when(F.col("measure") == "temp_c", F.col("value"))).alias("temp_c_total_value"),
    )
)
