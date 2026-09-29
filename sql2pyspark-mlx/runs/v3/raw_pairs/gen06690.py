from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.method").isin(["paypal", "promptpay"]))
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.max(F.col("p.paid_at")).alias("latest_paid_at"),
        F.count(F.col("p.amount")).alias("amount_count"),
    )
    .orderBy("status")
    .limit(20)
)
