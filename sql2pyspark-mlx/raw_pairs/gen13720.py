from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["New York", "Singapore", "Chiang Mai"]) & F.col("store_name").like("%Central%"))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
    )
)
