from pyspark.sql import functions as F

joined = (
    df_payments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select("e.*", F.col("o.amount").alias("order_amount"))
)

result = joined.select("id", "method", "amount", "paid_at", "status", "order_amount")
