from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("product_id") == 3) & ((F.col("last_restock") < "2024-07-01") | (F.col("store_id") == 5)))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
