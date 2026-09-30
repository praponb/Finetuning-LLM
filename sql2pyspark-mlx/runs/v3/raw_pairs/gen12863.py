from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "left")
    .select(
        F.col("p.order_id"),
        F.col("o.channel"),
        F.col("s.opened_date"),
        F.col("s.city"),
    )
)
