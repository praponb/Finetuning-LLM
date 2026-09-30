from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.pct_paypal") >= 67)
    .select(F.col("o.customer_id"), F.col("os.pct_paypal"), F.col("os.status_list"))
    .orderBy(F.col("customer_id").desc())
)
