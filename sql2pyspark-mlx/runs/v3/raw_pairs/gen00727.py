from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("sqft").isNotNull())

summary = (
    valid_stores
    .groupBy("region")
    .agg(F.count("sqft").alias("sqft_count"))
)

result = summary.filter(F.col("sqft_count") >= 3)
