from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 162)
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.floor("quantity").alias("quantity_whole"),
    )
)
