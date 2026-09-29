from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.month(F.col("o.order_date")).isin([2, 4, 8]))
    .groupBy(F.date_format(F.col("o.order_date"), "yyyy-MM").alias("order_month"))
    .agg(F.min(F.col("p.paid_at")).alias("min_paid_at"))
)
