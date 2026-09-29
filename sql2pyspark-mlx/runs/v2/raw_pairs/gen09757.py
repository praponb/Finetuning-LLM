from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("quantity") <= 146) & F.col("product_id").isin([2, 8]) & (F.year("last_restock") == 2024))
    .select(
        "id",
        "store_id",
        "quantity",
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)
