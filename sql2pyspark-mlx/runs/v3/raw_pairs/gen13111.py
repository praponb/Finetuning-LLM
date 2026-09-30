from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.discount") < 0.28) & (F.col("o.shipping_fee") >= 28))
    .select(
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("p.paid_at"),
        F.col("p.order_id"),
        F.col("p.amount").cast("int").alias("amount_int"),
    )
    .orderBy(F.col("customer_id").desc(), "paid_at")
)
