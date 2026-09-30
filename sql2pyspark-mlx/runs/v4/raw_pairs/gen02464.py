from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["card", "bank_transfer", "promptpay"]))
    .select(
        "amount",
        "status",
        F.when(F.col("method") == "paypal", 1).otherwise(0).alias("is_paypal"),
    )
    .orderBy("status", F.col("is_paypal").desc())
)
