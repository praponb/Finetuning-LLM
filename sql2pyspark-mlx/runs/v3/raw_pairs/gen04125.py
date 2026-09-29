from pyspark.sql import functions as F

base = df_stores.filter((F.length("store_name") > 10) & (F.col("sqft") == 9180))

stores_stats = (
    base
    .groupBy("city", F.month("opened_date").alias("opened_month"))
    .agg(F.max("sqft").alias("max_sqft"))
)

result = (
    stores_stats
    .orderBy("max_sqft", "city", "opened_month")
    .limit(3)
)
