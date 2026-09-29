from pyspark.sql import functions as F

min_amount = df_transactions.filter(F.col("account_id").isin([3, 8, 30, 40])).agg(F.min("amount")).first()[0]

result = (
    df_transactions
    .filter(F.col("amount") < min_amount)
    .select("id", "amount", "merchant", "txn_type")
    .orderBy("amount", F.col("id").desc())
    .limit(50)
)
