from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "left")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "right")
    .select(
        F.col("payments.amount"),
        F.col("orders.discount"),
        F.col("orders.shipping_fee"),
        F.col("customers.country"),
        F.col("customers.email"),
    )
    .orderBy(F.col("discount").desc())
)
