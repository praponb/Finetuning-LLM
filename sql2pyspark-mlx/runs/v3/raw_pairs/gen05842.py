from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id") == 4)
    .groupBy(F.month("last_restock").alias("last_restock_month"), "store_id")
    .agg(
        F.min("last_restock").alias("first_last_restock"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)
