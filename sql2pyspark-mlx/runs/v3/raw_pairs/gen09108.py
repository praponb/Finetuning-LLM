from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.month(F.col("ss.sale_date")).isin([2, 3, 10]))
    .groupBy(F.col("ss.product_id"))
    .agg(
        F.min(F.col("i.quantity")).alias("min_quantity"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
)
