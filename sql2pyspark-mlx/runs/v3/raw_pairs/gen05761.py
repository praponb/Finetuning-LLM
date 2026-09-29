from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([5, 25, 30, 33]))
    .select("id", "amount", "txn_type")
    .orderBy("id")
    .limit(10)
)
