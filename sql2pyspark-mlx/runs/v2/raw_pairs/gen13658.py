from pyspark.sql import functions as F

valid_stores = df_stores.filter((F.col("region") == "West") & F.upper("store_name").like("%ALL%"))

agg = (
    valid_stores
    .groupBy("region", "city")
    .agg(F.max("sqft").alias("max_sqft"))
)

result = agg.orderBy(F.col("region").desc())
