from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .filter((F.col("site_id") <= 7) & (F.col("read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

labeled = (
    scoped
    .select(
        "id",
        "kwh",
        "quality_ref",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.length(F.trim(F.col("quality_ref"))).alias("quality_ref_len"),
        F.concat(F.col("quality_ref"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref_ref"),
    )
)

grouped = (
    labeled
    .groupBy("quality_or_unknown", "quality_ref_len")
    .agg(
        F.round(F.avg("quality_ref_len"), 2).alias("avg_quality_ref_len"),
        F.round(F.stddev("quality_ref_len"), 2).alias("stddev_quality_ref_len"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("kwh"), 2).alias("avg_kwh"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("quality_ref_len", 0.5).alias("median_quality_ref_len"),
    )
)

result = (
    grouped
    .groupBy("n_rows")
    .pivot("quality_or_unknown", ["EST", "UNKNOWN", "BAD"])
    .agg(F.avg("median_quality_ref_len"))
)
