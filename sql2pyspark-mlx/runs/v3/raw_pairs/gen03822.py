from pyspark.sql import functions as F

max_balance = df_accounts.agg(F.max("balance")).first()[0]

result = (
    df_accounts
    .filter(F.col("balance") > max_balance)
    .select("id", "balance")
)
