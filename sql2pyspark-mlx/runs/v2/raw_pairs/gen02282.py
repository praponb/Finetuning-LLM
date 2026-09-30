from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("o.shipping_fee") > 21) & (F.length(F.col("c.name")) < 11))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.status"),
        F.col("o.channel"),
        F.col("o.amount"),
        F.col("c.name"),
        F.col("c.signup_date"),
    )
)
