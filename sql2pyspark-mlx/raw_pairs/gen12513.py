from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2023-09-28", "2024-07-01") | (F.length("store_name") > 8))
    .withColumn("region_store_name_key", F.concat_ws("/", "region", "store_name"))
    .withColumn("sqft_whole", F.floor("sqft"))
    .orderBy("city")
)
