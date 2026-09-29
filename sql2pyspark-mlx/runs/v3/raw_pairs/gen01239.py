from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 5) | F.col("store_id").isin([1, 2, 3, 5]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.col("quantity").cast("string").alias("quantity_num"),
    )
)
