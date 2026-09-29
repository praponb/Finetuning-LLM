from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") <= "2024-09-05") & (F.col("quantity") != 126))
    .groupBy("product_id", "store_id")
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
