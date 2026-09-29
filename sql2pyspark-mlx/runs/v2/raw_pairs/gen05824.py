from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3, 4]) & F.col("store_id").isin([3, 4, 5, 6]) & (F.year("last_restock") == 2025))
    .groupBy("product_id", "store_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 2)
    .orderBy("latest_last_restock", "store_id")
)
