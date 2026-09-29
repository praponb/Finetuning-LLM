from pyspark.sql import functions as F

filtered_sites = (
    df_sites
    .filter((F.col("installed_kw") < 284.0) | (F.col("zone") != "Z2"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_trunc("week", F.col("commissioned_on")).alias("commissioned_on_week"),
        F.month(F.col("commissioned_on")).alias("commissioned_on_month"),
    )
)

enriched = (
    filtered_sites
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on_week",
        "commissioned_on_month",
        F.add_months(F.col("commissioned_on"), 6).alias("commissioned_on_plus_6m"),
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
)

scored = enriched.withColumn("tariff_or_unknown", F.coalesce(F.col("tariff"), F.lit("UNKNOWN")))

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality").isNotNull(), 1)).alias("reading_quality_count"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    scored.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["reading_count", "reading_quality_count", "tariff"], ["reading_count"], ["reading_quality_count"], []],
        "reading_count", "reading_quality_count", "tariff",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("reading_quality_count").alias("max_reading_quality_count"),
    )
    .select(
        "reading_count",
        "reading_quality_count",
        F.coalesce(F.col("tariff"), F.lit("ALL")).alias("tariff"),
        "distinct_ids",
        "max_reading_quality_count",
    )
    .orderBy(
        F.col("reading_count").asc_nulls_last(),
        F.col("reading_quality_count").asc_nulls_last(),
        F.col("tariff").asc_nulls_last(),
    )
)
