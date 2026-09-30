from pyspark.sql import functions as F

site_side = (
    df_sites
    .select(
        F.lit("site").alias("source"),
        F.col("tariff").alias("category"),
        F.col("site_name").alias("category_2"),
    )
)

site_side_2 = (
    df_sites
    .filter((F.col("commissioned_on") < F.lit("2025-05-10").cast("date")) & (F.upper(F.trim(F.col("site_name"))).like("D%")))
    .select(F.lit("site_2").alias("source"), "tariff", "site_name")
)

result = (
    site_side
    .union(site_side_2)
    .orderBy(F.col("source").desc())
)
