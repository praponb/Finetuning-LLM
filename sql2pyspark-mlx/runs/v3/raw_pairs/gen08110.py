from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 3) | (F.col("quantity") > 69))
    .select(
        "last_restock",
        "store_id",
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
    .orderBy("last_restock_day", F.col("store_id").desc())
)
