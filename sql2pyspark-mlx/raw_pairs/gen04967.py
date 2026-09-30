from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.year("last_restock") >= 2024)
    .select(
        "id",
        "store_id",
        "product_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
