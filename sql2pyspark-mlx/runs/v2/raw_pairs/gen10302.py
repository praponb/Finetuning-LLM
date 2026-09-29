from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.avg_amount") >= 65)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("os.cnt"),
        F.col("os.avg_amount"),
    )
    .orderBy("order_date")
)
