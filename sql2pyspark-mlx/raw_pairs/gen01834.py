from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee").between(11, 26))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
    )
    .orderBy(F.col("channel").desc())
)
