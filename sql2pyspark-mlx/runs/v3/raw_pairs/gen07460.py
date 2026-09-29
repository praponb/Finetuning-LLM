from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 4, 6, 10]))
    .select(
        "id",
        "store_id",
        "last_restock",
        "product_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
