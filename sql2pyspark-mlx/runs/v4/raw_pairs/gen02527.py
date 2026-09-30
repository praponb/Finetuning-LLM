from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.order_id").isin([13, 18, 33]) & F.col("o.store_id").isNull())
    .select(
        F.col("o.amount"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("p.status"),
    )
)
