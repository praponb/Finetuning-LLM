from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.length("store_name") < 13) & (F.col("city") == "London")))
    .withColumn("city_store_name", F.concat("city", F.lit(" - "), "store_name"))
    .withColumn("city_lower", F.lower("city"))
    .orderBy("opened_date")
)
