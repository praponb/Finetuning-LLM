from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("discount") <= 0.36).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("oi.order_id").isin([1, 32]))
    .select(F.col("oi.id"), F.col("oi.product_id"))
    .orderBy("id", F.col("product_id").desc())
)
