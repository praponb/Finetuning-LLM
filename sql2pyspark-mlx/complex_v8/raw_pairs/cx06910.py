from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("kwh") < 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

site_id_stats = (
    cleaned_readings
    .groupBy("site_id")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

outliers = (
    cleaned_readings.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

stats = (
    outliers
    .filter(F.col("voltage") >= 216.5)
    .groupBy(
        F.when(F.col("kwh") >= 82.9, "HIGH").when(F.col("kwh") >= 48.2, "MEDIUM").otherwise("LOW").alias("kwh_tier"),
    )
    .agg(F.count("*").alias("row_count"), F.max("voltage").alias("max_voltage"))
    .filter(F.col("row_count") >= 2)
)

screened = (
    stats
    .crossJoin(stats.agg(F.avg("max_voltage").alias("overall_avg_max_voltage")))
    .filter(F.col("max_voltage") > F.col("overall_avg_max_voltage"))
    .drop("overall_avg_max_voltage")
)

result = (
    screened
    .select(
        "kwh_tier",
        "max_voltage",
        F.least(F.col("max_voltage"), F.col("row_count")).alias("least_max_voltage_row_count"),
        F.coalesce(F.col("kwh_tier"), F.lit("UNKNOWN")).alias("kwh_tier_or_unknown"),
    )
    .orderBy(F.col("kwh_tier_or_unknown").asc(), "max_voltage", "kwh_tier")
    .limit(10)
)
