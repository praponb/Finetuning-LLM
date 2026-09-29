from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 8, 9]) & F.col("product_id").isin([1, 5, 9, 10]))
    .select(
        "last_restock",
        "store_id",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)
