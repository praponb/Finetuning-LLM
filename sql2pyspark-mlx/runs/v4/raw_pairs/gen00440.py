from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.col("p.paid_at") < "2023-08-03")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("s.store_name"),
    )
)
