from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 216.1)
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
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

joined = (
    with_parent_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-11-28").cast("date")))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.quality",
        "s.site_tariff",
        "s.site_installed_kw",
        "s.site_commissioned_on",
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
    )
)

result = (
    joined
    .rollup("site_id", "site_tariff")
    .agg(
        F.sum(F.when(F.col("quality") != "EST", 1).otherwise(0)).alias("n_quality_est"),
        F.max("read_at").alias("last_read_at"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("site_tariff"), F.lit("ALL")).alias("site_tariff"),
        "n_quality_est",
        "last_read_at",
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("site_tariff").asc_nulls_last(),
    )
)
