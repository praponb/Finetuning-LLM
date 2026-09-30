from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.month("last_restock").isin([1, 4, 11]) & (F.col("store_id") == 2)) | (F.col("last_restock") <= "2025-07-12"))
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
