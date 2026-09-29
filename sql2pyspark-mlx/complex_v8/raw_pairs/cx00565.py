from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality").isin("BAD", "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")).alias("quality_ref"),
    )
)

enriched = (
    cleaned_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality_ref",
        F.col("si.zone").alias("site_zone"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") != "Z2") & (F.col("si.tariff").isin("FLAT", "TOU")), "left_semi")
)

with_parent_site = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality_ref",
        "s.site_zone",
        "s.site_tariff",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_details_site = (
    with_parent_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone_2"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
    )
)

result = (
    with_details_site
    .filter(F.col("site_installed_kw") > 93.3)
    .select(
        "id",
        "quality_ref",
        "site_zone",
        "site_tariff",
        "site_installed_kw_2",
    )
    .orderBy(F.col("site_zone").desc(), "id")
    .limit(10)
)
