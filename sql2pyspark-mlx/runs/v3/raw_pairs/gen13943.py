from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("o.shipping_fee").isNotNull())
    .groupBy(F.month(F.col("o.order_date")).alias("order_month"))
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
    .orderBy(F.col("max_quantity").desc())
)
