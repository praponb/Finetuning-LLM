from pyspark.sql import functions as F

site_rows = df_sites.select(F.col("zone").alias("label"), F.col("commissioned_on").alias("day"))

site_side = (
    df_sites
    .filter(F.col("installed_kw") >= 96.9)
    .select(F.col("zone").alias("label"), F.col("commissioned_on").alias("day"))
)

result = (
    site_rows
    .unionByName(site_side)
    .orderBy("day")
)
