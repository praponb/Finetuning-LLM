from pyspark.sql import functions as F

site_side = (
    df_sites
    .select(
        F.col("id").alias("key_id"),
        "commissioned_on",
        F.lit("site").alias("origin"),
    )
)

site_side_2 = df_sites.select("id", "commissioned_on", F.lit("site_2").alias("origin"))

result = (
    site_side
    .union(site_side_2)
    .distinct()
    .orderBy("key_id", "origin")
)
