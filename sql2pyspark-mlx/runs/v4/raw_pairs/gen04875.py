from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.to_date(F.col("p.paid_at")) >= "2025-06-01")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.shipping_fee"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("order_id").desc())
)
