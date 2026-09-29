from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "id",
        "last_restock",
        "store_id",
        "product_id",
        F.floor("quantity").alias("quantity_whole"),
    )
)
