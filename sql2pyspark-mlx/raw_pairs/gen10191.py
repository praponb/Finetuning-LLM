from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 6, 10]) & (F.col("quantity") <= 184) & F.col("store_id").isin([3, 4, 5, 6]))
    .select(
        "id",
        "store_id",
        "quantity",
        "product_id",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
        F.floor("quantity").alias("quantity_whole"),
    )
)
