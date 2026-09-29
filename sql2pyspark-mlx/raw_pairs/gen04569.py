from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .filter(F.col("ss.store_id").isin([3, 4]))
    .groupBy(
        F.when(F.col("s.sqft") >= 15627, "high").when(F.col("s.sqft") >= 5779, "mid").otherwise("low").alias("sqft_bucket"),
        F.col("s.city"),
    )
    .agg(F.sum(F.col("ss.revenue")).alias("total_revenue"))
)
