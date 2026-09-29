from pyspark.sql import functions as F

result = (
    df_accounts
    .select(
        "id",
        "status",
        "account_type",
        "balance",
        F.round("balance", 1).alias("balance_rounded"),
    )
)
