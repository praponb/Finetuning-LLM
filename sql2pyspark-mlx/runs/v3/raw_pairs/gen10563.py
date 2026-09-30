from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([1, 3]) & (F.col("store_id").isin([2, 4, 5]) | (F.col("quantity") == 214)))
    .select(
        "last_restock",
        "store_id",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
    .orderBy("last_restock_month_end")
)
