from pyspark.sql import functions as F

recent_sites = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 4) & (F.col("zone").isNotNull()))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.when(F.col("installed_kw") >= 373.3, "HIGH").when(F.col("installed_kw") >= 96.9, "MEDIUM").otherwise("LOW").alias("installed_kw_tier"),
        F.when(F.col("installed_kw") >= 386.6, "large").when(F.col("installed_kw") >= 141.6, "medium").otherwise("small").alias("installed_kw_tier_2"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isin("OK", "EST"), 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

summarized_rows = (
    recent_sites.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
    )
)

qualified = (
    summarized_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh").between(35.0, 117.1)), "left_anti")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count_2"))
)

profiled = (
    qualified.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

result = (
    profiled
    .groupingSets(
        [["tariff", "zone"], ["tariff"], ["zone"], []],
        "tariff", "zone",
    )
    .agg(
        F.round(F.avg("installed_kw"), 2).alias("avg_installed_kw"),
        F.percentile_approx("reading_count", 0.5).alias("median_reading_count"),
        F.min("first_reading_read_at").alias("first_first_reading_read_at"),
    )
    .select(
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        F.coalesce(F.col("zone"), F.lit("ALL")).alias("zone"),
        "avg_installed_kw",
        "median_reading_count",
        "first_first_reading_read_at",
    )
    .orderBy(F.col("tariff").asc_nulls_last(), F.col("zone").asc_nulls_last())
)
