from pyspark.sql import functions as F

site_side = df_sites.select("site_name", F.col("tariff").alias("label"))

site_side_2 = df_sites.select(F.col("tariff").alias("site_name"), F.col("site_name").alias("label"))

combined = (
    site_side
    .unionByName(site_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("site_name")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("site_name")
)
