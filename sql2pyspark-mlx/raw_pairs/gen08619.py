from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5]) & F.col("product_id").isin([2, 8, 9]))
    .select(
        "quantity",
        "store_id",
        "last_restock",
        F.date_sub("last_restock", 48).alias("last_restock_minus_48d"),
    )
    .orderBy("last_restock_minus_48d")
)
