from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([1, 3, 4]))
    .select("txn_type")
    .distinct()
)
