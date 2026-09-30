from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.to_date(F.col("p.paid_at")).alias("paid_at_date"),
    )
    .orderBy("paid_at_date")
)
