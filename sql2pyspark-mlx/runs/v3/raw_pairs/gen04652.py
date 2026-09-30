from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("region").isin(["West", "North"]) & (F.length("store_name") > 11))
)

summary = (
    filtered_stores
    .groupBy("city")
    .agg(
        F.max("sqft").alias("max_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
)

result = summary
