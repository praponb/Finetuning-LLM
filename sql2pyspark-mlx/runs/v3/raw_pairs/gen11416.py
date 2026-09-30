from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Singapore", "Bangkok", "Tokyo"]))
    .withColumn("city_region_key", F.concat_ws("-", "city", "region"))
    .withColumn("city_store_name", F.concat("city", F.lit(", "), "store_name"))
    .orderBy("opened_date", "store_name")
)
