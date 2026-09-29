from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([13, 19, 20]))
    .select("id", "txn_type")
)
