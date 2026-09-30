from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 6]) & (F.col("store_id") == 2))
    .select(
        "id",
        "quantity",
        "last_restock",
        F.date_sub("last_restock", 62).alias("last_restock_minus_62d"),
    )
    .orderBy("id")
)
