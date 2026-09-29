from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([6, 10]) & F.col("p.method").isin(["cash", "card", "promptpay"]))
    .select(
        F.col("p.paid_at"),
        F.col("p.status"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("o.order_date"),
    )
)
