from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee").between(11, 23) & (F.col("p.order_id") == 4))
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.customer_id"),
        F.col("o.amount").alias("order_amount"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(20)
)
