from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("order_id") == 34)
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("method") == "card", 1)).alias("card_count"))
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "left")
    .filter(F.col("order_stats.card_count") > 6)
    .select(
        F.col("orders.amount"),
        F.col("orders.channel"),
        F.col("order_stats.card_count"),
    )
    .orderBy(F.col("channel").desc())
)
