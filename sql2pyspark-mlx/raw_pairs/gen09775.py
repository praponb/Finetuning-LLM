from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "left")
    .filter(F.col("orders.shipping_fee") > 26)
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("payments.status"),
        F.col("orders.channel"),
        F.col("customers.signup_date"),
        F.col("customers.country"),
    )
    .orderBy("channel", "id")
    .limit(50)
)
