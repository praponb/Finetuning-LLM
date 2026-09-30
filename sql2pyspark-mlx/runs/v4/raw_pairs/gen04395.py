from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 7, 10]) & F.col("product_id").isin([1, 3, 8]))
    .select(
        "id",
        "product_id",
        "store_id",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_num"),
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
    )
)
