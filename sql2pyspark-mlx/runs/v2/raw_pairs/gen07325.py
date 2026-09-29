from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("region").isin(["West", "East"]))

summary = (
    filtered_stores
    .groupBy("region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.count("*").alias("total_count"),
    )
)

result = (
    summary
    .filter(F.col("total_sqft") >= 55000)
    .select("region", "total_sqft", "total_count")
)
