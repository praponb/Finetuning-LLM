from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("account_id") == 19) & (F.col("amount") >= 4000) & F.col("account_id").isin([5, 6, 11, 20]))
    .select(
        "id",
        "account_id",
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)
