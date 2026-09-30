from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isin([23, 25, 29, 35]))
    .select(F.col("p.paid_at"), F.col("o.customer_id"), F.col("o.order_date"))
    .orderBy(F.col("order_date").desc())
)
