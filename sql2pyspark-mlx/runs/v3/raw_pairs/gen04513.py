from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") >= "2024-03-21")
    .select(
        "balance",
        "status",
        "account_type",
        F.round((F.col("balance") * 0.9), 2).alias("balance_adjusted"),
    )
)
