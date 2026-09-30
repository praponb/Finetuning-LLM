from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Air%") & ~F.col("region").isin(["West", "East", "South"]))
    .agg(
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.count("sqft").alias("sqft_count"),
    )
)
