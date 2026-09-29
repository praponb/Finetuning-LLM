from pyspark.sql import functions as F

base = df_stores.filter((F.col("city") == "London") & F.lower("store_name").like("%ive%"))

stores_stats = (
    base
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("sqft")).otherwise(0)).alias("chiang_mai_sqft"),
        F.count("*").alias("total_count"),
    )
)

result = (
    stores_stats
    .filter(F.col("chiang_mai_sqft") < 34)
    .orderBy("chiang_mai_sqft")
)
