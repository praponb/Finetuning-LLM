from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("D%"))
    .select("id", F.col("tariff").alias("category"))
)

site_rows_2 = df_sites.select("id", F.col("site_name").alias("category"))

result = (
    site_rows
    .intersect(site_rows_2)
    .orderBy("category", "id")
)
