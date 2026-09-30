from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") <= 18000)
    .select(
        "customer_id",
        "account_type",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
