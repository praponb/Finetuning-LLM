from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter((F.col("o.amount") < 450) & F.col("p.order_id").isNull())
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.paid_at"),
        F.col("o.amount"),
        F.col("o.order_date"),
        F.col("o.status"),
        F.concat(F.col("o.channel"), F.lit(" - "), F.col("o.status")).alias("channel_status"),
    )
)
