from pyspark.sql import functions as F

avg_balance = df_accounts.filter(F.col("opened_date") <= "2025-10-31").agg(F.avg("balance")).first()[0]

result = (
    df_accounts
    .filter(F.col("balance") > avg_balance)
    .select("id", "balance")
)
