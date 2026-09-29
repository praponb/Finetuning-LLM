from pyspark.sql import functions as F

base = (
    df_sites
    .filter((F.col("site_name").isNotNull()) & (F.col("installed_kw") < 386.6))
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
    )
)

derived = (
    base
    .filter(F.col("commissioned_on") >= F.lit("2024-10-28").cast("date"))
    .select(
        "*",
        F.concat(F.col("zone"), F.lit("#"), F.col("id").cast("string")).alias("zone_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
)

enriched = (
    derived
    .select(
        "id",
        "zone",
        "commissioned_on",
        "zone_ref",
        "zone_slug",
        F.abs(F.col("installed_kw") - 284.0).alias("installed_kw_distance"),
        F.when(F.col("installed_kw") >= 373.3, "A").when(F.col("installed_kw") >= 141.6, "B").otherwise("C").alias("installed_kw_tier"),
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    enriched.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

result = (
    with_stats
    .groupingSets(
        [["zone", "installed_kw_tier"], ["zone"]],
        "zone", "installed_kw_tier",
    )
    .agg(
        F.percentile_approx("installed_kw_distance", 0.5).alias("median_installed_kw_distance"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
    )
    .orderBy(
        F.col("zone").asc_nulls_last(),
        F.col("installed_kw_tier").asc_nulls_last(),
    )
)
