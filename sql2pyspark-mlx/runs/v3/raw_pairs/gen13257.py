from pyspark.sql import functions as F

avg_balance = df_accounts.filter(F.col("opened_date") <= "2023-06-24").agg(F.avg("balance")).first()[0]

result = (
    df_accounts
    .filter(F.col("balance") > avg_balance)
    .select("id", "balance", "customer_id")
    .orderBy(F.col("balance").desc())
)
