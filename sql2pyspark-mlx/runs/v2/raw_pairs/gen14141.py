from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(F.col("city").isin(["New York", "Singapore"]) & (F.col("region") == "West"))
)

stores_stats = (
    filtered_stores
    .groupBy("region")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)

result = (
    stores_stats
    .filter(F.col("sqft_range") < 21)
    .orderBy(F.col("region").desc())
)
