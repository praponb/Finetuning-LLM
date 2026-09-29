from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-04-01")
    .groupBy("order_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .select(F.col("o.status"), F.col("ot.amount_count"))
)
