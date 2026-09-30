from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") == "SAVINGS") & (F.col("balance") <= 28000))
    .select(
        "account_type",
        "balance",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled_2"),
    )
)
