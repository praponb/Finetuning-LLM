from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter((F.col("p.method") != "cash") & F.col("p.status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.shipping_fee"),
        F.col("c.country"),
    )
)
