from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([5, 6]))
    .groupBy("store_id", F.quarter("last_restock").alias("last_restock_quarter"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.max("last_restock").alias("latest_last_restock"),
    )
    .orderBy("latest_last_restock", "store_id", "last_restock_quarter")
    .limit(5)
)
