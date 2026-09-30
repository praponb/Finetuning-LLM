from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 9]) & (F.col("store_id").isin([1, 4, 5, 6]) | (F.col("product_id") == 7)))
    .select(
        "id",
        "last_restock",
        "store_id",
        F.round((F.col("quantity") * 1.1), 2).alias("quantity_scaled"),
    )
)
