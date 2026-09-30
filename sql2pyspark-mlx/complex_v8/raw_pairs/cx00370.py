from pyspark.sql import functions as F

site_rows = (
    df_sites
    .filter(F.col("zone") == "Z2")
    .select(
        F.col("tariff").alias("name"),
        F.col("zone").alias("label"),
        F.col("commissioned_on").alias("day"),
        F.lit("site").alias("source"),
    )
)

site_side = (
    df_sites
    .filter((F.upper(F.trim(F.col("site_name"))).like("D%")) & (F.col("zone") == "Z2"))
    .select(
        F.col("site_name").alias("name"),
        F.col("zone").alias("label"),
        F.col("commissioned_on").alias("day"),
        F.lit("site_2").alias("source"),
    )
)

result = (
    site_rows
    .unionByName(site_side)
    .orderBy("source")
)
