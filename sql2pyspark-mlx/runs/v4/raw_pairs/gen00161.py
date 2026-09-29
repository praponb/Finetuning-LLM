from pyspark.sql import functions as F

metrics_long = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "temp_c",
        F.col("humidity").alias("HUMIDITY"),
        F.col("pressure").alias("PRESSURE"),
    )
    .unpivot(["id", "device_code"], ["temp_c", "HUMIDITY", "PRESSURE"], "kpi", "value")
    .filter(F.col("value").isNotNull())
)

result = (
    metrics_long
    .withColumn("value_tier", F.when(F.col("value") >= 90, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["kpi", "value_tier"], ["kpi"], []], "kpi", "value_tier")
    .agg(
        F.percentile_approx("value", [0.25, 0.5, 0.75]).alias("value_quartiles"),
        F.round(F.avg("value"), 2).alias("avg_value"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_rows"),
        F.min("value").alias("min_value"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("value_tier").desc())
)
