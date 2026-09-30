from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5, 6]) & F.col("product_id").isin([3, 5, 8, 9]))
    .select(
        "last_restock",
        "product_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
