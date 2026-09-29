from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("i.store_id").isin([2, 3]))
    .groupBy(F.col("ss.product_id"), F.col("ss.store_id"))
    .agg(
        F.avg(F.col("i.quantity")).alias("avg_quantity"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("avg_quantity") >= 60)
    .orderBy("stddev_quantity")
)
