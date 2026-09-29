from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.countDistinct("amount").alias("unique_methods"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.unique_methods") >= 4)
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("os.unique_methods"),
        F.col("os.refunded_amount"),
    )
)
