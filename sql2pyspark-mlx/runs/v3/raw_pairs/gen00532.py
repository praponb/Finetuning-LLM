from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.method").isin(["paypal", "card"]) & ~F.col("p.status").isin(["SUCCESS", "FAILED"]))
    .select(
        F.col("o.amount"),
        F.col("o.shipping_fee"),
        F.col("p.status"),
        F.col("p.amount").alias("payment_amount"),
        F.col("p.paid_at"),
    )
    .orderBy(F.col("amount").desc())
)
