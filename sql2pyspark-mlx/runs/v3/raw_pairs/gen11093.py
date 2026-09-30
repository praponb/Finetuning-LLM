from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .filter(F.col("i.store_id").isin([3, 4]))
    .groupBy(F.col("ss.product_id"), F.col("ss.store_id"))
    .agg(F.round(F.stddev(F.col("i.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc(), F.col("product_id").desc(), "store_id")
    .limit(10)
)
