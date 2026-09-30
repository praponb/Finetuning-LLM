from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(~F.col("method").isin(["promptpay", "card"]))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        F.when(F.col("method").isin(["bank_transfer", "promptpay"]), "Priority").otherwise("Other").alias("method_group"),
        F.date_trunc("YEAR", "paid_at").alias("paid_at_period"),
    )
)

result = (
    filtered
    .select("order_id")
    .exceptAll(
        df_payments
        .filter(F.col("method") != "promptpay")
        .select("order_id")
    )
)
