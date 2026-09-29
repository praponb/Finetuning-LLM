from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2025-07-10") & ((F.col("quantity") >= 172) | (F.col("store_id") == 2)))
    .select(
        "id",
        "product_id",
        "quantity",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)
