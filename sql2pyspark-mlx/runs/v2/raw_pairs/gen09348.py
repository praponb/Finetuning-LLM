from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.status").isin(["RETURNED", "CANCELLED", "PENDING"]))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.carrier"),
        F.col("o.shipping_fee"),
        F.col("c.credit_limit"),
        F.col("c.city"),
    )
)
