from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("zone").isin("Z3", "Z4")) & (F.col("installed_kw") <= 373.3))
    .select(F.col("id").alias("entity_id"), "commissioned_on")
)

site_side_2 = df_sites.select(F.col("id").alias("entity_id"), "commissioned_on")

result = site_side.unionByName(site_side_2)
