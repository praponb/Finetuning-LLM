from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-11-01") & F.col("method").isin(["card", "bank_transfer", "cash"]))
    .agg(
        F.max("amount").alias("max_amount"),
        F.min("amount").alias("min_amount"),
        F.min("paid_at").alias("min_paid_at"),
    )
    .orderBy("max_amount")
)
