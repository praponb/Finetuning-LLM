from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.discount") < 0.11)
    .select(
        F.col("p.id"),
        F.col("p.paid_at"),
        F.col("p.order_id"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("o.channel"),
    )
)
