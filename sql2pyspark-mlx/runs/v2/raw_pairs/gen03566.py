from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(F.col("p.order_id") == 10)
    .select(
        F.col("p.status"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
    )
)
