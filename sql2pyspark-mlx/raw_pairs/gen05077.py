from pyspark.sql import functions as F

min_balance = df_accounts.agg(F.min("balance")).first()[0]

result = (
    df_accounts
    .filter(F.col("balance") == min_balance)
    .select("id", "balance", "status", "account_type")
)
