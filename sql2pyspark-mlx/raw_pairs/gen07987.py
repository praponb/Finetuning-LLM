from pyspark.sql import functions as F

filtered_stores = df_stores.filter((F.col("sqft") <= 14993) & F.col("store_name").like("Si%"))

agg = (
    filtered_stores
    .groupBy(
        F.when(F.col("sqft") >= 12487, "high").when(F.col("sqft") >= 7711, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.sum("sqft").alias("total_sqft"))
)

result = (
    agg
    .filter(F.col("total_sqft") > 73000)
    .orderBy(F.col("sqft_bucket").desc(), F.col("total_sqft").desc())
)
