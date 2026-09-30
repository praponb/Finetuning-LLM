from pyspark.sql import functions as F

site_rows = df_sites.select(F.col("id").alias("ref_id"), "zone")

site_side = df_sites.select("id", "site_name")

result = (
    site_rows
    .union(site_side)
    .orderBy("ref_id")
)
