from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(F.min(F.col("p.paid_at")).alias("min_paid_at"))
    .orderBy(F.col("min_paid_at").desc())
)
