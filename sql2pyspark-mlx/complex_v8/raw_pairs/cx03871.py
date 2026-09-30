from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 232.0, "large").when(F.col("voltage") >= 218.4, "medium").otherwise("small").alias("voltage_tier"),
        F.round(F.col("voltage") * 0.25, 2).alias("voltage_adj"),
    )
)

per_group = (
    filtered
    .groupBy("voltage_tier")
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.sum(F.when(F.col("quality").isNotNull(), 1).otherwise(0)).alias("n_quality_is_not_null"),
        F.round(100 * F.count(F.when(F.col("quality") == "OK", 1)) / F.count("*"), 1).alias("pct_quality_ok"),
    )
)

labeled = (
    per_group
    .filter(F.col("pct_quality_ok") >= 30.0)
    .select(
        "voltage_tier",
        "first_read_at",
        "n_quality_is_not_null",
        "pct_quality_ok",
        F.round(F.col("n_quality_is_not_null") / F.nullif(F.col("pct_quality_ok"), F.lit(0)), 3).alias("n_quality_is_not_null_per_pct_quality_ok"),
    )
)

labeled_2 = (
    labeled
    .filter(F.col("pct_quality_ok") <= 85.0)
    .withColumn("voltage_tier_prefix", F.substring(F.trim(F.col("voltage_tier")), 1, 2))
)

result = (
    labeled_2
    .groupBy("n_quality_is_not_null")
    .pivot("voltage_tier", ["small", "medium"])
    .agg(F.count("n_quality_is_not_null_per_pct_quality_ok"))
)
