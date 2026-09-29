from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.channel").isin(["web", "mobile"]))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.paid_at"),
        F.col("o.store_id"),
        F.col("c.segment"),
    )
    .orderBy(F.col("paid_at").desc(), "id")
    .limit(20)
)
