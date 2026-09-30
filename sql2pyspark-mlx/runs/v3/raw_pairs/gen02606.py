from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 4, 5]) & F.month("last_restock").isin([4, 7, 11]))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
    .orderBy("store_id")
)
