from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1100)
    .select(
        "id",
        "account_id",
        "is_flagged",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
    )
)
