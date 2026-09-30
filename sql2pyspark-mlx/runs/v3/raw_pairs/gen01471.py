from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("method") != "paypal")
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", F.col("amount")).otherwise(0)).alias("bank_transfer_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.bank_transfer_amount") >= 22)
    .select(
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("ot.bank_transfer_amount"),
    )
    .orderBy("customer_id", "store_id")
)
