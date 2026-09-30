from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.amount") <= 1220)
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
    )
)
