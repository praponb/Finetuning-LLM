from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.to_date(F.col("p.paid_at")) < "2023-11-12")
    .select(F.col("p.id"), F.col("p.amount"), F.col("o.discount"), F.col("s.city"))
)
