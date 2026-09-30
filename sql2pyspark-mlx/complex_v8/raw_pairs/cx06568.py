from pyspark.sql import functions as F

scoped_sites = df_sites.filter(~F.col("tariff").isin("FLAT", "TOU"))

site_side = scoped_sites.select(F.col("site_name").alias("name"), "zone", "commissioned_on")

site_rows = (
    df_sites
    .filter((F.length(F.trim(F.col("site_name"))) >= 7) & (F.col("tariff") == "TOU"))
    .select("site_name", "tariff", "commissioned_on")
)

result = (
    site_side
    .union(site_rows)
    .distinct()
    .orderBy(F.col("name").desc())
)
