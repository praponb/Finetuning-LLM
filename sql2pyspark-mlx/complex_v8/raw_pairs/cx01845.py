from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

joined = (
    prepared_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") == "TOU"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_clean",
        "s.quality_prefix",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

with_parent_site = (
    joined.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_prefix",
        "s.site_installed_kw",
        F.col("si.installed_kw").alias("site_installed_kw_2"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_parent_site
    .groupBy("site_id")
    .pivot("quality", ["EST", "BAD", "OK"])
    .agg(F.sum("site_installed_kw_2"))
)
