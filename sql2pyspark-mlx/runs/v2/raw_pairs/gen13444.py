from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity").isNull() & F.col("product_id").isin([2, 7, 10])) | (F.col("last_restock") > "2024-11-18"))
    .select(
        "quantity",
        "last_restock",
        "store_id",
        F.round("quantity", 0).alias("quantity_rounded"),
    )
)
