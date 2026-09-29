from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .filter(F.col("ss.store_id").isin([1, 3, 5, 6]))
    .groupBy(F.col("ss.store_id"))
    .agg(
        F.count(F.col("i.quantity")).alias("quantity_count"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("quantity_count").desc())
)
