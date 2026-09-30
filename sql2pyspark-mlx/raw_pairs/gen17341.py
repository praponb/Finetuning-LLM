from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") >= 76000) & (F.col("status") == "CLOSED"))
    .select(
        "id",
        "balance",
        "opened_date",
        F.lower("account_type").alias("account_type_lower"),
    )
)
