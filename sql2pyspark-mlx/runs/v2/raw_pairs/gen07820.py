from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(F.col("ss.product_id"))
    .agg(
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
        F.sum(F.col("i.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "product_id")
    .limit(5)
)
