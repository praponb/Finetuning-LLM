from pyspark.sql import functions as F

min_amount = df_transactions.filter(F.col("txn_time") >= "2024-12-04").agg(F.min("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") <= min_amount)
    .select("id", "amount", "txn_time")
    .orderBy("amount", F.col("id").desc())
)
