from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

with_parent = (
    valid.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.site_name").isNotNull())
    .select(
        "s.*",
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_details = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") > 93.3)
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
        F.col("si.site_name").alias("site_site_name"),
    )
)

joined_site = (
    with_details.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw_3"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    joined_site
    .groupBy("read_at", "quality")
    .pivot("site_tariff", ["FLAT", "TOU", "EV"])
    .agg(F.avg("site_installed_kw"))
    .orderBy("read_at", "quality")
)
