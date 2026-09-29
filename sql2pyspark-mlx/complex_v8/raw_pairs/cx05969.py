from pyspark.sql import functions as F

site_side = df_sites.select(F.col("id").alias("key_id"), "commissioned_on")

site_side_2 = (
    df_sites
    .filter(F.year(F.col("commissioned_on")) == 2025)
    .select("id", "commissioned_on")
)

site_side_3 = (
    df_sites
    .filter(F.col("tariff").isNotNull())
    .select("id", "commissioned_on")
)

result = (
    site_side
    .union(site_side_2)
    .union(site_side_3)
    .orderBy(F.col("key_id").desc(), "commissioned_on")
)
