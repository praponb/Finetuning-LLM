from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([4, 5]))
    .select(
        "quantity",
        "last_restock",
        "store_id",
        F.dayofmonth("last_restock").alias("last_restock_day"),
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
    .orderBy(F.col("store_id").desc())
)
