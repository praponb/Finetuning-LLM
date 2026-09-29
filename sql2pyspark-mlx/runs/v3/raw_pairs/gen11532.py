from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.channel"),
        F.col("s.opened_date"),
    )
)
