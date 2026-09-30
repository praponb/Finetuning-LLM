from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("customer_id").isin([10, 30, 32, 40])).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("oi.product_id").isNotNull())
    .select(F.col("oi.id"), F.col("oi.product_id"))
)
