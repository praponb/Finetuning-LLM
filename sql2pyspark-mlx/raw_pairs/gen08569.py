from pyspark.sql import functions as F

min_amount = df_transactions.filter(F.col("merchant") == "7-Eleven").agg(F.min("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") <= min_amount)
    .select("id", "amount", "txn_type")
    .orderBy("txn_type")
)
