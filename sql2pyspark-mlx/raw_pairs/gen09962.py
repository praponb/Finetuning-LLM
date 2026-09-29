from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city")
    .agg(
        F.count("*").alias("total_count"),
        F.count("*").alias("store_count"),
        F.max("sqft").alias("max_sqft"),
    )
    .filter(F.col("max_sqft") > 11500)
    .drop("max_sqft")
)
