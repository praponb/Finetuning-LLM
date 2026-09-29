from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 7, 10]) & F.col("product_id").isin([1, 6, 7, 8]) & (F.col("last_restock") >= "2024-03-29"))
    .select(
        "id",
        "quantity",
        "last_restock",
        "store_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
