from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]))
    .groupBy("order_id")
    .agg(
        F.count("*").alias("num_payments"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.channel"), F.col("os.num_payments"), F.col("os.method_values"))
    .orderBy("channel", F.col("method_values").desc())
)
