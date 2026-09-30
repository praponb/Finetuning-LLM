from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.month("opened_date").isin([2, 3, 6]) & (F.col("region").isNull() | (F.col("region") == "South")))
)

agg = (
    filtered_stores
    .groupBy("region")
    .agg(F.avg("sqft").alias("avg_sqft"))
)

result = (
    agg
    .select("region", "avg_sqft")
    .orderBy(F.col("region").desc())
)
