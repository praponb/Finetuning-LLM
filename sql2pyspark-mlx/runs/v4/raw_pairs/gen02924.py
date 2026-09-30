from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isin([27, 30]) & (F.col("p.amount") <= 1530))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.status").alias("order_status"),
    )
)
