from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 4, 6]) & (F.col("store_id").isin([3, 4]) | F.col("product_id").isin([3, 10])))
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.count("quantity").alias("quantity_count"),
    )
)
