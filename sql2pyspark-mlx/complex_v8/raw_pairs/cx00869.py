from pyspark.sql import functions as F

cleaned = (
    df_meter_readings
    .filter(F.col("voltage") > 214.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

enriched_site = (
    cleaned.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

result = (
    enriched_site
    .groupBy("read_at", "site_id")
    .pivot("quality", ["OK", "EST", "BAD"])
    .agg(F.avg("site_installed_kw"))
    .orderBy("read_at", "site_id")
)
