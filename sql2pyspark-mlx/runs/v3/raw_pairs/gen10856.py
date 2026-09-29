from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("payments.method").isin(["paypal", "cash"]))
    .select(
        F.col("payments.id"),
        F.col("payments.status"),
        F.col("payments.amount"),
        F.col("payments.method"),
        F.col("orders.order_date"),
        F.col("orders.channel"),
        F.col("orders.amount").alias("order_amount"),
    )
)
