from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 5]) & (F.col("last_restock") >= "2024-08-01"))
    .groupBy("store_id", "product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum("quantity").alias("total_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
)
