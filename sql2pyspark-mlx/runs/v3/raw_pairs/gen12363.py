from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 59000) | F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .select(
        "id",
        "account_type",
        F.upper("account_type").alias("account_type_upper"),
        F.trim("account_type").alias("clean_account_type"),
    )
)
