from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .groupBy(F.col("orders.status"))
    .agg(
        F.count(F.when(F.col("payments.method") == "paypal", 1)).alias("paypal_count"),
        F.min(F.col("payments.amount")).alias("min_amount"),
    )
)
