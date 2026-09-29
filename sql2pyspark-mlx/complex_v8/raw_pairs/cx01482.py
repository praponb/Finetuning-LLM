from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 229.9, "large").when(F.col("voltage") >= 219.5, "medium").otherwise("small").alias("voltage_tier"),
    )
)

quality_stats = (
    valid_readings
    .groupBy("quality")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

above_average = (
    valid_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

tagged = (
    above_average
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        (F.col("kwh") - F.col("median_kwh")).alias("kwh_minus_median_kwh"),
    )
)

result = (
    tagged
    .select(
        "id",
        F.col("median_kwh").alias("median_kwh_v"),
        F.col("kwh").alias("m_kwh"),
        F.col("kwh_minus_median").alias("m_kwh_minus_median"),
    )
    .unpivot(
        ids=["id"],
        values=["median_kwh_v", "m_kwh", "m_kwh_minus_median"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
