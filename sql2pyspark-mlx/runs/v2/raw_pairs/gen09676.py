from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") != "FROZEN")
    .select(
        "account_type",
        "balance",
        "opened_date",
        F.round((F.col("balance") * 1.07), 2).alias("adjusted_balance"),
    )
    .orderBy(F.col("adjusted_balance").desc(), F.col("balance").desc())
)
