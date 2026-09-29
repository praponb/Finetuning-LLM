from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("o.shipping_fee") <= 11) & (F.col("p.status") == "SUCCESS"))
    .select(
        F.col("p.amount"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("o.shipping_fee"),
    )
)
