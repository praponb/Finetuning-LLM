from pyspark.sql import functions as F

max_amount = df_transactions.filter(F.col("is_flagged")).agg(F.max("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") >= max_amount)
    .select("id", "amount", "txn_time", "txn_type")
)
