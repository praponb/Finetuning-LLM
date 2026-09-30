from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("order_id") == 30)
    .groupBy("order_id")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.discount"),
        F.col("o.channel"),
        F.col("os.status_list"),
        F.col("os.total_amount"),
    )
    .orderBy(F.col("discount").desc())
)
