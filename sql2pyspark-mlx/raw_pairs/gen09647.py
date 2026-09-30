from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 2) | (F.col("quantity") <= 120))
    .select(
        "id",
        "last_restock",
        "product_id",
        "store_id",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)
