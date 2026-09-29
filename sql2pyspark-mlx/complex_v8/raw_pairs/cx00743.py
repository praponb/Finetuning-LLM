from pyspark.sql import functions as F

scoped_sites = (
    df_sites
    .filter((F.year(F.col("commissioned_on")) == 2025) & (F.upper(F.trim(F.col("site_name"))).like("D%")))
)

site_rows = (
    scoped_sites
    .select(
        "id",
        F.col("tariff").alias("category"),
        F.col("commissioned_on").alias("day"),
    )
)

site_side = (
    df_sites
    .filter(F.col("tariff") != "FLAT")
    .select(
        "id",
        F.col("tariff").alias("category"),
        F.col("commissioned_on").alias("day"),
    )
)

stacked = site_rows.union(site_side)

result = (
    stacked
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("category")
)
