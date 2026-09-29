from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 8]) & F.col("product_id").isin([5, 6, 7]))
    .select(
        "last_restock",
        "store_id",
        "product_id",
        F.floor("quantity").alias("quantity_whole"),
    )
)
