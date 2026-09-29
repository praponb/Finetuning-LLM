from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("status") == "FROZEN") & ((F.col("account_type") == "CREDIT") | (F.col("account_type") == "CHECKING")))
    .select(
        "id",
        "account_type",
        "opened_date",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
    )
    .orderBy("account_type")
)
