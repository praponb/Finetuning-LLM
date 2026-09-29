from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("store_name_region_key", F.concat_ws("/", "store_name", "region"))
    .withColumn("region_store_name", F.concat("region", F.lit(" - "), "store_name"))
)
