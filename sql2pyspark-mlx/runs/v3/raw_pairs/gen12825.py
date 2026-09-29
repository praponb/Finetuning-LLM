from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("order_id").isin([17, 20, 21, 24]))
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.success_count") > 9)
    .select(
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("t.success_count"),
        F.col("t.card_count"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
