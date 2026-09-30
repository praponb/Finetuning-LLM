from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([2, 3, 4, 7]) & F.col("product_id").isin([5, 9, 10]))
    .cube("product_id", "store_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("last_restock").alias("last_last_restock"),
        F.countDistinct("quantity").alias("unique_stores"),
    )
)
