from pyspark.sql import functions as F

max_amount = df_transactions.filter(F.col("txn_time") >= "2024-09-18").agg(F.max("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") > max_amount)
    .select("id", "amount", "account_id")
)
