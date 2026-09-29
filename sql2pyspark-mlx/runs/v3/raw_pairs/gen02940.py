from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 3100)
    .select(
        "account_id",
        "amount",
        "merchant",
        F.lower("merchant").alias("merchant_lower"),
    )
)
