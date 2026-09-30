from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.filter(F.col("order_id").isin([10, 11, 29, 36])).alias("oi"), F.col("o.id") == F.col("oi.order_id"), "left_semi")
    .filter(F.col("o.store_id").isin([5, 13, 23, 38]))
    .select(F.col("o.id"), F.col("o.discount"))
    .orderBy(F.col("discount").desc())
)
