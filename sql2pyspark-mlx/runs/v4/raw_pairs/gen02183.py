from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 5, 7, 10]) & (F.year("last_restock") == 2024) & (F.col("store_id") == 5))
    .agg(
        F.count("quantity").alias("quantity_count"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.min("last_restock").alias("first_last_restock"),
    )
    .orderBy("first_last_restock")
)
