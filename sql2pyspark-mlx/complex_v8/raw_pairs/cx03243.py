from pyspark.sql import functions as F

base = (
    df_sites
    .filter(F.col("tariff").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.concat(F.col("zone"), F.lit("#"), F.col("id").cast("string")).alias("zone_ref"),
    )
)

site_side = base.select(F.col("id").alias("qty"), "commissioned_on")

site_rows = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(F.col("id").alias("qty"), "commissioned_on")
)

site_side_2 = (
    df_sites
    .filter((F.lower(F.col("site_name")).like("% n%")) & (F.col("installed_kw") < 446.9))
    .select(F.col("id").alias("qty"), "commissioned_on")
)

stacked = (
    site_side
    .union(site_rows)
    .union(site_side_2)
)

result = (
    stacked
    .groupBy("qty")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("commissioned_on").alias("latest_commissioned_on"),
    )
    .orderBy("qty")
)
