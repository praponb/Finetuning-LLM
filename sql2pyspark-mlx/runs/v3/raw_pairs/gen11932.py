from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 6]) & (F.col("store_id") == 4) & (F.col("store_id") == 3))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("last_restock").alias("last_last_restock"),
    )
)
