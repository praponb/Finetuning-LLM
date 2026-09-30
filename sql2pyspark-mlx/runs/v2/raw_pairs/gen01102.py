from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.credit_limit") <= 6700) & F.col("c.email").like("%@gmail.com"))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("s.delivered_date"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("c.name"),
        F.col("c.is_active"),
    )
    .orderBy("name")
)
