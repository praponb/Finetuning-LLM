from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter((F.col("site_id") >= 2) | (F.col("quality") != "EST"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.abs(F.col("voltage") - 229.1).alias("voltage_distance"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

qualified = (
    cleaned_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") >= 52.4), "left_semi")
)

screened = (
    qualified.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(7, 13)) & (F.col("m.quality") == "BAD"), "left_semi")
    .filter(F.col("quality").isNotNull())
)

combined_details_site = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.quality_prefix",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

rollup_base = (
    combined_details_site
    .groupBy("site_id", "site_zone")
    .agg(
        F.count(F.when(~F.col("site_tariff").isin("FLAT", "EV"), 1)).alias("n_site_tariff_not_in_flat_ev"),
        F.array_sort(F.collect_set("site_tariff")).alias("site_tariff_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("quality") == "BAD", 1).otherwise(0)).alias("n_quality_bad"),
    )
)

scored = (
    rollup_base
    .select(
        "*",
        F.substring(F.trim(F.col("site_zone")), 1, 4).alias("site_zone_prefix"),
        F.initcap(F.trim(F.col("site_zone"))).alias("site_zone_clean"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_counts = (
    scored.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_counts
    .select(
        "site_id",
        "n_site_tariff_not_in_flat_ev",
        "site_tariff_list",
        "row_count",
        "site_zone_prefix",
        "site_zone_clean",
    )
    .orderBy("site_zone_clean", F.col("site_zone_prefix").desc())
)
