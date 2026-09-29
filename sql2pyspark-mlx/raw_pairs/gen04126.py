from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.filter(F.col("paid_at") < "2023-05-03").alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.customer_id") == 5)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.discount"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
