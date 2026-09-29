from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 3, 4]) & (F.col("product_id").isin([1, 2, 7]) | (F.col("store_id") == 2)))
    .select(
        "id",
        "product_id",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
        F.floor("quantity").alias("quantity_whole"),
    )
    .orderBy(F.col("id").desc())
)
