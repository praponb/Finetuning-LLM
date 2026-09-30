from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("order_id").isin([8, 15, 17, 19]))
    .groupBy("order_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.amount_range") > 26)
    .select(F.col("o.discount"), F.col("ot.amount_range"), F.col("ot.pct_cash"))
)
