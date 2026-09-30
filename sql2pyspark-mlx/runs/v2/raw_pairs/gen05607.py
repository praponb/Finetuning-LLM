from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        F.sum(F.when(F.col("p.method") == "paypal", F.col("p.amount")).otherwise(0)).alias("paypal_amount"),
    )
)
