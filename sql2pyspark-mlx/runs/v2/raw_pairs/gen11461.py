from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.email").like("%@outlook.com"))
    .select(
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("c.name"),
        F.col("c.is_active"),
        F.col("c.age"),
    )
)
