from pyspark.sql import functions as F

min_balance = df_accounts.filter(F.col("customer_id") == 23).agg(F.min("balance")).first()[0]

result = (
    df_accounts
    .filter(F.col("balance") == min_balance)
    .select("id", "balance")
    .orderBy(F.col("id").desc())
)
