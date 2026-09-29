from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.to_date(F.col("p.paid_at")) < "2023-04-01")
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.col("o.status").alias("order_status"),
    )
)
