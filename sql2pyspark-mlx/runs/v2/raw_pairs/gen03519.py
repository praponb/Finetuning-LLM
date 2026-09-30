from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2024-10-28")
    .select(
        "id",
        "product_id",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
