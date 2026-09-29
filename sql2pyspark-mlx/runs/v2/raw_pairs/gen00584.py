from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id") == 2)
    .select(
        "id",
        "product_id",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)
