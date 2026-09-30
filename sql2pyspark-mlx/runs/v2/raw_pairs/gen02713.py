from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .select(
        "id",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.date_trunc("MONTH", "last_restock").alias("last_restock_period"),
    )
    .orderBy(F.col("id").desc())
)
