from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.year(F.col("orders.order_date")) == 2023)
    .select(
        F.col("payments.paid_at"),
        F.col("payments.order_id"),
        F.col("payments.amount"),
        F.col("orders.customer_id"),
        F.col("orders.amount").alias("order_amount"),
        F.col("orders.store_id"),
        F.hour(F.col("payments.paid_at")).alias("paid_at_hour"),
    )
    .orderBy(F.col("paid_at_hour").desc(), F.col("paid_at").desc())
)
