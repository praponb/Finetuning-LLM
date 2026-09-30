from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter((F.col("zone").isin("Z3", "Z2")) & (F.length(F.trim(F.col("site_name"))) >= 7))
    .select(F.col("id").alias("ref_id"), F.col("commissioned_on").alias("day"))
)

site_side_2 = df_sites.select(F.col("id").alias("ref_id"), F.col("commissioned_on").alias("day"))

site_side_3 = df_sites.select(F.col("id").alias("ref_id"), F.col("commissioned_on").alias("day"))

result = (
    site_side
    .unionByName(site_side_2)
    .unionByName(site_side_3)
)
