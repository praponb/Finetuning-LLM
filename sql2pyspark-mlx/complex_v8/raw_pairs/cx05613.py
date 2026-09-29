from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter(F.col("voltage") <= 241.9)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

joined_site = (
    recent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality_slug",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

joined_site_2 = (
    joined_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.site_installed_kw",
        F.col("si.zone").alias("site_zone_2"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

with_children = (
    joined_site_2.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.site_installed_kw",
        "s.site_tariff",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    with_children
    .groupBy("reading_read_at")
    .pivot("site_tariff", ["FLAT", "TOU", "EV"])
    .agg(F.max("site_installed_kw"))
)
