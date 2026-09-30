from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~(F.col("region").isin(["West", "East"]) & F.col("region").isin(["West", "North"]) & (F.col("sqft") < 12177)))
    .groupBy("region")
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.min("sqft").alias("min_sqft"),
    )
)
