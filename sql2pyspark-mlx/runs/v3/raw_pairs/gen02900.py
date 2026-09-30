from pyspark.sql import functions as F

filtered_stores = (
    df_stores
    .filter(~F.col("region").isin(["East", "West", "South"]) & ((F.col("sqft") == 8398) | (F.col("region") == "West")))
)

summary = (
    filtered_stores
    .groupBy("city", "region")
    .agg((F.max("sqft") - F.min("sqft")).alias("sqft_range"))
)

result = summary.select("city", "region")
