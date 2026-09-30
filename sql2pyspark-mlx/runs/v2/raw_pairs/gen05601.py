from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") == "North") & F.col("store_name").like("A%"))
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        F.sum("sqft").alias("total_sqft"),
        F.count("sqft").alias("sqft_count"),
    )
)
