from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(F.min(F.col("p.paid_at")).alias("min_paid_at"))
)
