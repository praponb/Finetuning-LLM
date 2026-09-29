from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .select(
        F.col("payments.id"),
        F.col("payments.amount"),
        F.col("payments.order_id"),
        F.col("orders.channel"),
        F.col("orders.status"),
        F.col("customers.age"),
        F.coalesce(F.col("orders.discount"), F.lit(0)).alias("discount_filled"),
    )
    .orderBy(F.col("status").desc())
)
