from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") <= 1150) | F.col("method").isNotNull())
    .select(
        "amount",
        "method",
        "status",
        F.to_date("paid_at").alias("paid_at_date"),
        F.when(F.col("method").isin(["promptpay", "bank_transfer"]), "Tier 1").otherwise("Other").alias("method_group"),
    )
)
