from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .filter(F.length(F.col("c.email")) < 13)
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.status"),
        F.col("o.order_date"),
        F.col("c.age"),
        F.concat_ws("-", F.col("o.status"), F.col("o.channel")).alias("status_channel_key"),
    )
)
