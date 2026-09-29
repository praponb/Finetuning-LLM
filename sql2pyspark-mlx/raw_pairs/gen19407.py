from pyspark.sql import functions as F

result = (
    df_inventory
    .select(
        "last_restock",
        "quantity",
        "store_id",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
    .orderBy(F.col("last_restock").desc())
)
