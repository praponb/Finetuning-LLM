from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 6]))
    .groupBy(
        F.when(F.col("quantity") >= 225, "high").when(F.col("quantity") >= 32, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("inventory_count"),
    )
    .filter(F.col("inventory_count") < 5)
    .drop("inventory_count")
    .orderBy(F.col("quantity_bucket").desc())
)
