from pyspark.sql import functions as F

site_side = df_sites.select("id", F.col("commissioned_on").alias("day"), "tariff")

site_rows = df_sites.select("id", "commissioned_on", "tariff")

stacked = site_side.union(site_rows)

result = (
    stacked
    .groupBy("tariff")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("tariff")
)
