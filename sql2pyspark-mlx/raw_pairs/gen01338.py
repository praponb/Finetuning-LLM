from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 1) & F.col("product_id").isin([5, 10]))
    .groupBy(
        F.when(F.col("quantity") >= 250, "high").when(F.col("quantity") >= 102, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("total_count"),
    )
    .orderBy("quantity_bucket")
    .limit(3)
)
