from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 4, 5]))
    .select(
        "id",
        "last_restock",
        "quantity",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
