from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("voltage") <= 240.9) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

with_details_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "si.site_name",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

combined_details_site = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.site_name",
        "s.site_zone",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone_2"),
        F.col("si.tariff").alias("site_tariff_2"),
    )
)

combined_details = (
    combined_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.site_name",
        "s.site_zone",
        "s.site_commissioned_on",
        "s.site_zone_2",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "site_id",
        "site_name",
        "site_zone",
        "site_commissioned_on",
        "site_zone_2",
        "site_installed_kw",
        F.date_format(F.col("site_commissioned_on"), "yyyy-MM-dd").alias("site_commissioned_on_label"),
        F.when(F.col("site_installed_kw") >= 373.3, "large").when(F.col("site_installed_kw") >= 96.9, "medium").otherwise("small").alias("site_installed_kw_tier"),
    )
    .orderBy(F.col("site_commissioned_on").desc())
)
