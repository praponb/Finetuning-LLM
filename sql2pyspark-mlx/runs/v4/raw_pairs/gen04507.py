from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.store_id").isin([17, 29]))
    .select(F.col("p.id"), F.col("p.paid_at"), F.col("p.amount"), F.col("o.order_date"))
)
