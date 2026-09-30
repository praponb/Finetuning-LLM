from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(((F.col("quantity") != 145) & (F.col("last_restock") >= "2023-04-30")) | (F.col("store_id") == 6))
    .select(
        "id",
        "product_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
