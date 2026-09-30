from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.order_id").isNull() & (F.col("o.store_id") == 38))
    .select(
        F.col("p.id"),
        F.col("p.status"),
        F.col("p.order_id"),
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.round((F.col("p.amount") * 1.15), 2).alias("amount_adjusted"),
    )
)
