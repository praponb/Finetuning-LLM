from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("order_totals"), F.col("orders.id") == F.col("order_totals.order_id"), "left")
    .filter(F.col("order_totals.card_amount") > 34)
    .select(
        F.col("orders.order_date"),
        F.col("order_totals.avg_amount"),
        F.col("order_totals.card_amount"),
    )
)
