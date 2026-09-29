from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.last_restock") < "2023-07-01")
    .groupBy(F.col("ss.store_id"), F.col("ss.product_id"))
    .agg(
        F.max(F.col("i.quantity")).alias("max_quantity"),
        F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("product_id").desc(), "store_id")
    .limit(10)
)
