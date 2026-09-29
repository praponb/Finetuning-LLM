from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("account_id").isin([8, 18, 19, 22]))
    .select("merchant", "txn_type")
    .distinct()
    .orderBy("merchant")
)
