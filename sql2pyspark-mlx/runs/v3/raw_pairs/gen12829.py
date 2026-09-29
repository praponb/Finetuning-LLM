from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]))
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(F.col("o.order_date"), F.col("o.discount"), F.col("O.refunded_amount"))
    .orderBy(F.col("refunded_amount").desc())
)
