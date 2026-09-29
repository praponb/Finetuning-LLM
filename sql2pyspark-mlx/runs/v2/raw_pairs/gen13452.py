from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 8)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)
