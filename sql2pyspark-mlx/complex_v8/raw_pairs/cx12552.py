from pyspark.sql import functions as F

site_rows = df_sites.select(F.col("id").alias("qty"))

site_side = df_sites.select(F.col("id").alias("qty"))

result = site_rows.intersect(site_side)
