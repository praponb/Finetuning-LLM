from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant", "txn_time", "amount")
    .orderBy("amount", "txn_time")
)
