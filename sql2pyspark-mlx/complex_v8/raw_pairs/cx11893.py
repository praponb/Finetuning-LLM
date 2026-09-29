from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter((F.col("voltage") < 231.7) & (F.col("read_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.year(F.col("read_at")).alias("read_at_year"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

without_matches = (
    cleaned.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") != "EST") & (F.col("m.read_at") < F.lit("2025-03-02 11:00:00").cast("timestamp")), "left_anti")
)

with_details_site = (
    without_matches.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") > 141.6)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.read_at_year",
        "s.quality_slug",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

joined_site = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff").isin("TOU", "EV")), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.read_at_year",
        "s.quality_slug",
        "s.site_installed_kw",
        "s.site_commissioned_on",
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw_2"),
    )
)

result = (
    joined_site
    .groupBy("site_zone")
    .pivot("quality", ["OK", "BAD", "EST"])
    .agg(F.sum("site_installed_kw"))
)
