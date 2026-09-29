from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~((F.col("quantity") <= 197) & (F.col("last_restock") >= "2025-05-01") & (F.col("last_restock") > "2024-02-16")))
    .select(
        "product_id",
        "store_id",
        "quantity",
        F.date_sub("last_restock", 18).alias("last_restock_minus_18d"),
    )
)
