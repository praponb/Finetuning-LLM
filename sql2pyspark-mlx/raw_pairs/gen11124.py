from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.to_date(F.col("p.paid_at")) >= "2025-06-01")
    .groupBy(F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"))
    .agg(F.max(F.col("p.amount")).alias("max_amount"))
    .orderBy(F.col("order_month").desc())
)
