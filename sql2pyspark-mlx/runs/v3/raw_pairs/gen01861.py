from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 269, "high").when(F.col("quantity") >= 38, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("inventory_count"),
        F.max("quantity").alias("max_quantity"),
    )
)
