from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "FROZEN")
    .select(
        "account_type",
        "opened_date",
        F.abs("balance").alias("abs_balance"),
        F.round("balance", 2).alias("balance_rounded"),
    )
)
