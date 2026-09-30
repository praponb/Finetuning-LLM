from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([14, 16]))
    .select(
        F.col("o.order_date"),
        F.col("o.status"),
        F.col("o.discount"),
        F.col("p.amount"),
        F.col("p.order_id"),
        F.col("p.method"),
    )
)
