from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "account_type",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
)
