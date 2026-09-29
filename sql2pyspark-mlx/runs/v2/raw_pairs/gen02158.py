from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 5]))
    .groupBy(F.date_format("last_restock", "yyyy-MM").alias("last_restock_month"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") > 27)
    .orderBy(F.col("stddev_quantity").desc(), "last_restock_month")
)
