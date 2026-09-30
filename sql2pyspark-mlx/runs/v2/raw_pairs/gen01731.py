from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "paypal", "bank_transfer"]))
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("ot.card_count"),
        F.col("ot.failed_amount"),
    )
)
