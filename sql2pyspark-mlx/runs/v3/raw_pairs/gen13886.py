from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "txn_time", "merchant", "account_id")
    .orderBy(F.col("account_id").desc(), "merchant")
)
