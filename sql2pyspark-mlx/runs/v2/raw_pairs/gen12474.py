from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.order_id"),
        F.col("o.shipping_fee"),
        F.col("c.is_active"),
        F.when(F.col("c.is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
    .orderBy(F.col("is_active_label").desc())
)
