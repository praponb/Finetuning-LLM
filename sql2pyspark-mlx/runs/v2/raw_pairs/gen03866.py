from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("order_id").isin([23, 29, 36]) & F.col("method").isin(["paypal", "cash", "bank_transfer"])))
    .select(
        "id",
        "amount",
        F.ceil("amount").alias("amount_whole"),
        F.hour("paid_at").alias("paid_at_hour"),
    )
)
