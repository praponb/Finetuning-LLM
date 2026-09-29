from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("o.amount") < 1210)
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.concat(F.col("o.channel"), F.lit(", "), F.col("o.status")).alias("channel_status"),
    )
)
