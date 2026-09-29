from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("payments.order_id").isNull())
    .groupBy(F.col("orders.channel"))
    .agg(
        F.count(F.when(F.col("payments.method") == "promptpay", 1)).alias("promptpay_count"),
        F.count(F.col("payments.amount")).alias("amount_count"),
    )
)
