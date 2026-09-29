from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("method").isin(["card", "paypal", "cash"]) | F.col("order_id").isNotNull())
    .select(
        "order_id",
        "amount",
        "status",
        F.floor("amount").alias("amount_whole"),
        F.date_trunc("year", "paid_at").alias("paid_at_period"),
    )
    .orderBy("status")
)
