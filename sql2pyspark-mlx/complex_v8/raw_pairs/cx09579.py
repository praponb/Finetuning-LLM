from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("tariff").isin("FLAT", "EV")) & (F.upper(F.trim(F.col("site_name"))).like("R%")))
    .select("zone", "tariff")
)

site_rows = df_sites.select(F.col("tariff").alias("zone"), F.col("zone").alias("tariff"))

result = (
    site_side
    .unionByName(site_rows)
    .orderBy(F.col("tariff").desc())
)
