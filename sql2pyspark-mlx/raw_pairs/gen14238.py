from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", "account_id")
    .pivot("merchant", ["Shell", "Amazon", "Grab"])
    .agg(F.max("amount"))
)
