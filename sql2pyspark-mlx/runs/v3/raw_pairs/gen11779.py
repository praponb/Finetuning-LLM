from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("p.method"),
    )
    .orderBy("amount")
)
