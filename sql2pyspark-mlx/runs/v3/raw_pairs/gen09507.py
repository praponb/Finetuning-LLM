from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("balance") <= 57000) & (F.col("account_type") == "LOAN") & F.col("balance").isNull())
    .select("id", "status", "account_type")
)
