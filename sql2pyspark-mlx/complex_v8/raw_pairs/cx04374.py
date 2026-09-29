from pyspark.sql import functions as F

recent_sites = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 8) & (F.col("installed_kw") <= 446.9))
    .select(
        "id",
        "zone",
        "tariff",
        "commissioned_on",
        F.concat(F.col("site_name"), F.lit("#"), F.col("id").cast("string")).alias("site_name_ref"),
    )
)

site_rows = (
    recent_sites
    .select(
        "tariff",
        "commissioned_on",
        F.col("zone").alias("label"),
        F.lit("site").alias("source"),
    )
)

site_side = (
    df_sites
    .filter(F.length(F.trim(F.col("site_name"))) >= 6)
    .select(
        "tariff",
        "commissioned_on",
        F.col("zone").alias("label"),
        F.lit("site_2").alias("source"),
    )
)

result = (
    site_rows
    .unionByName(site_side)
    .orderBy(F.col("commissioned_on").desc(), "source")
)
