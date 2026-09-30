from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "bank_transfer", "paypal"]))
    .select(
        "id",
        "amount",
        "paid_at",
        F.when(F.col("status").isin(["SUCCESS", "REFUNDED"]), "Tier 1").otherwise("Other").alias("status_group"),
    )
)
