from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter(F.upper("store_name").like("%IAM%"))
    .withColumn("city_lower", F.lower("city"))
)

stores_stats = (
    valid_stores
    .groupBy("city")
    .agg(F.min("opened_date").alias("min_opened_date"))
)

result = stores_stats
