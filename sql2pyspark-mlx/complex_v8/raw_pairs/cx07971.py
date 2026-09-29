from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("voltage") < 240.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

with_details_site = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") <= 386.6)
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality",
        "s.quality_clean",
        "s.quality_prefix",
        "s.quality_slug",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

joined = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.quality_clean",
        "s.quality_prefix",
        "s.site_name",
        "s.site_commissioned_on",
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
    )
)

result = (
    joined
    .groupBy("site_commissioned_on")
    .pivot("quality", ["BAD", "EST", "OK"])
    .agg(F.sum("kwh").alias("total"), F.max("kwh").alias("max_v"))
    .orderBy("site_commissioned_on")
)
