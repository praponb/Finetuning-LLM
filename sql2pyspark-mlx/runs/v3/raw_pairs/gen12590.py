from pyspark.sql import functions as F

filtered_stores = df_stores.filter((F.col("sqft") > 15444) & (F.col("region") == "East"))

region_summary = (
    filtered_stores
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "New York", 1).otherwise(0)).alias("new_york_count"),
        F.count("sqft").alias("sqft_count"),
    )
)

result = region_summary.orderBy(F.col("sqft_count").desc())
