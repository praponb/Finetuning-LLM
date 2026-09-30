from pyspark.sql import functions as F

site_side = df_sites.select(F.col("id").alias("ref_id"), F.col("commissioned_on").alias("day"))

site_rows = df_sites.select("id", "commissioned_on")

result = site_side.union(site_rows)
