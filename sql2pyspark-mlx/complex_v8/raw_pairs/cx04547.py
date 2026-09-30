from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("installed_kw") > 141.6) & (F.col("zone").isNotNull()))
    .select("id", F.col("zone").alias("name"), "commissioned_on")
)

site_rows = (
    df_sites
    .filter((F.col("installed_kw") > 141.6) & (F.col("site_name").isNotNull()))
    .select("id", F.col("site_name").alias("name"), "commissioned_on")
)

site_rows_2 = (
    df_sites
    .filter((F.col("site_name").isNotNull()) & (F.col("tariff").isin("FLAT", "EV")))
    .select("id", F.col("tariff").alias("name"), "commissioned_on")
)

result = (
    site_side
    .unionByName(site_rows)
    .unionByName(site_rows_2)
    .orderBy("commissioned_on", "id")
)
