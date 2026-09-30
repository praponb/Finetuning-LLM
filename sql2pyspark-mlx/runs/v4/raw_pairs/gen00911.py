from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2025-05-01") & ((F.year("opened_date") >= 2024) | (F.col("city") == "Berlin")))
    .withColumn("store_name_region", F.concat("store_name", F.lit(" - "), "region"))
    .withColumn("region_title", F.initcap(F.trim("region")))
    .orderBy("region_title")
)
