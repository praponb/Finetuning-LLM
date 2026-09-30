from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity") <= 176)
    .select(
        "id",
        "product_id",
        F.dayofmonth("last_restock").alias("last_restock_day"),
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
    )
)
