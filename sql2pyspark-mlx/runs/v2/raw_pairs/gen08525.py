from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("order_id") == 32).alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.customer_id"),
    )
)
