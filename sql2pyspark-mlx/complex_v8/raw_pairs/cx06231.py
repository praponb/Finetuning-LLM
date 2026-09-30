from pyspark.sql import functions as F

site_rows = (
    df_sites
    .select(
        F.lit("site").alias("source"),
        F.col("zone").alias("name"),
        F.col("site_name").alias("name_2"),
        "commissioned_on",
    )
)

site_side = (
    df_sites
    .filter(F.col("tariff") != "EV")
    .select(
        F.lit("site_2").alias("source"),
        F.col("zone").alias("name"),
        F.col("site_name").alias("name_2"),
        "commissioned_on",
    )
)

result = (
    site_rows
    .unionByName(site_side)
    .orderBy(F.col("name").desc(), "commissioned_on")
)
