from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") < "2023-07-17") & (F.col("txn_type") != "CREDIT") & F.col("amount").between(1400, 2500))
    .select(
        "is_flagged",
        F.coalesce("merchant", F.lit("Unknown")).alias("merchant_filled"),
    )
)
