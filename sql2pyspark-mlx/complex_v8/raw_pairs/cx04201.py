from pyspark.sql import functions as F

prepared_sites = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
    )
)

site_side = prepared_sites.select(F.col("zone").alias("name"), F.col("site_name").alias("category"))

site_side_2 = df_sites.select(F.col("site_name").alias("name"), F.col("tariff").alias("category"))

site_side_3 = df_sites.select(F.col("tariff").alias("name"), F.col("zone").alias("category"))

result = (
    site_side
    .unionByName(site_side_2)
    .unionByName(site_side_3)
    .orderBy("category", "name")
)
