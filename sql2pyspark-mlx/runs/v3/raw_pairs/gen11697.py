from pyspark.sql import functions as F

min_amount = df_transactions.filter(F.hour("txn_time").between(6, 14)).agg(F.min("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") == min_amount)
    .select("id", "amount")
    .orderBy("id", F.col("amount").desc())
)
