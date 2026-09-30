from pyspark.sql import functions as F

avg_amount = df_transactions.filter(F.col("account_id") == 24).agg(F.avg("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") >= avg_amount)
    .select("id", "amount", "is_flagged", "txn_type")
    .orderBy("txn_type")
)
