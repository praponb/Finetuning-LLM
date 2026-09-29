from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .select(
        F.col("s.order_id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("c.country"),
        F.col("c.signup_date"),
    )
    .orderBy("order_id", "shipping_fee")
)
