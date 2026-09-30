from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.channel").isin(["partner", "store", "mobile"]))
    .select(
        F.col("p.paid_at"),
        F.col("p.order_id"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.status"),
    )
    .orderBy("paid_at")
)
