from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .groupBy(F.col("orders.channel"))
    .agg(F.min(F.col("payments.paid_at")).alias("first_paid_at"))
    .orderBy(F.col("first_paid_at").desc(), "channel")
    .limit(5)
)
