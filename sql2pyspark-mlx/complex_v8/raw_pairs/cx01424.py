from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter(F.col("kwh") >= 14.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_parent_site = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.tariff").alias("site_tariff"), "si.site_name")
)

joined_site = (
    with_parent_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") < 373.3)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.site_tariff",
        "s.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("kwh").alias("total_reading_kwh"),
        F.count("*").alias("reading_count"),
    )
)

profiled = (
    joined_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_kwh",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

enriched = (
    profiled
    .select(
        "*",
        F.greatest(F.col("reading_count"), F.col("site_installed_kw")).alias("greatest_reading_count_site_installed_kw"),
        F.when(F.col("site_tariff").isin("FLAT", "EV"), "group_a").when(F.col("site_tariff") == "TOU", "group_b").otherwise("other").alias("site_tariff_group"),
    )
)

result = (
    enriched
    .filter(F.col("reading_count") >= 2)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "site_tariff",
        "site_name",
        "site_installed_kw",
        "site_tariff_2",
        "total_reading_kwh",
        "greatest_reading_count_site_installed_kw",
        "site_tariff_group",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
    .orderBy(F.col("voltage").desc_nulls_first(), "site_id")
)
