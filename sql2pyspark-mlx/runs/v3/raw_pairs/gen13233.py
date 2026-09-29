from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("p.order_id").isin([5, 8, 25]) & F.col("o.store_id").isin([7, 12, 17]))
    .select(F.col("p.paid_at"), F.col("o.customer_id"), F.col("o.discount"))
    .orderBy("discount")
)
