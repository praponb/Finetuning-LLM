from pyspark.sql import functions as F

valid_inventory = (
    df_inventory
    .filter(F.month("last_restock").isin([5, 7, 9]) & F.col("store_id").isin([2, 3, 5]) & (F.col("last_restock") <= "2025-08-17"))
)

summary = (
    valid_inventory
    .groupBy(
        F.when(F.col("quantity") >= 229, "high").when(F.col("quantity") >= 92, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    summary
    .filter(F.col("avg_quantity") < 180)
    .orderBy(F.col("quantity_bucket").desc(), "product_id")
    .limit(10)
)
