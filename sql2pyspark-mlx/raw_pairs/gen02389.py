from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .groupBy(F.col("c.is_active"), F.col("c.segment"))
    .agg(
        F.max(F.col("o.shipping_fee")).alias("max_shipping_fee"),
        F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"),
    )
)
