from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("amount") < 430)
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("P"), F.col("o.id") == F.col("P.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("P.pct_success"),
        F.col("P.promptpay_count"),
    )
)
