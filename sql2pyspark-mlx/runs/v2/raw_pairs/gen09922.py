from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.avg_amount") >= 630)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("os.unique_statuss"),
        F.col("os.avg_amount"),
    )
    .orderBy(F.col("id").desc())
)
