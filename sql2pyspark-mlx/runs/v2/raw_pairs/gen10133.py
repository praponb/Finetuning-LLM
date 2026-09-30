from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("region") == "East")

summary = (
    filtered_stores
    .groupBy("city")
    .agg(F.countDistinct("sqft").alias("unique_citys"))
)

result = summary.select("city", "unique_citys")
