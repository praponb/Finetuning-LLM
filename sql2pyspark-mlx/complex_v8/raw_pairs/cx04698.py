from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (F.col("site_id").isNotNull()))
)

enriched = (
    prepared
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("voltage") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("voltage_per_kwh"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

quality_stats = (
    enriched
    .groupBy("quality")
    .agg(F.avg("kwh").alias("avg_kwh"))
)

outliers = (
    enriched.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh") > F.col("g.avg_kwh"))
    .select(
        "s.*",
        "g.avg_kwh",
        F.round(F.col("s.kwh") - F.col("g.avg_kwh"), 2).alias("kwh_minus_avg"),
    )
)

qualified = (
    outliers
    .crossJoin(outliers.agg(F.avg("kwh_minus_avg").alias("overall_avg_kwh_minus_avg")))
    .filter(F.col("kwh_minus_avg") > F.col("overall_avg_kwh_minus_avg"))
    .drop("overall_avg_kwh_minus_avg")
)

scored = (
    qualified
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "voltage_per_kwh",
        "quality_len",
        "avg_kwh",
        "kwh_minus_avg",
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

result = (
    scored
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "quality_len",
        "kwh_minus_avg",
        "read_at_year",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.abs(F.col("voltage") - 227.3).alias("voltage_distance"),
    )
    .orderBy("voltage", "id")
    .limit(5)
)
