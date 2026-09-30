from pyspark.sql import functions as F

metrics_long = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY"),
        F.col("temp_c").alias("TEMP_C_VAL"),
    )
    .unpivot(["id", "device_code"], ["HUMIDITY", "TEMP_C_VAL"], "metric", "val")
    .filter(F.col("val").isNotNull())
)

result = (
    metrics_long
    .groupBy(F.when(F.col("val") >= 68, "LARGE").otherwise("SMALL").alias("val_tier"))
    .agg(
        F.min("val").alias("min_val"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("val") / F.nullif(F.count(F.when(F.col("device_code") == "DV-04", 1)), F.lit(0)), 2).alias("val_per_dv_04"),
        F.count("*").alias("row_count"),
        F.percentile_approx("val", 0.95).alias("val_p95"),
    )
    .filter(F.col("distinct_ids") >= 1)
    .orderBy("val_tier")
)
