from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

enriched_site = (
    scoped.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.upper(F.trim(F.col("si.site_name"))).like("S%")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

joined = (
    enriched_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.site_commissioned_on",
        "s.site_zone",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    joined
    .groupBy("site_commissioned_on", "site_id")
    .pivot("site_zone", ["Z2", "Z4", "Z3"])
    .agg(F.sum("kwh").alias("amount"), F.count("kwh").alias("n"))
    .orderBy("site_commissioned_on", "site_id")
)
