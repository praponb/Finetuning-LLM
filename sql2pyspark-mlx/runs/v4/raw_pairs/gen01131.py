from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.amount").between(860, 1050) | (F.to_date(F.col("p.paid_at")) < "2023-10-01"))
    .select(F.col("p.order_id"), F.col("o.store_id"))
)
