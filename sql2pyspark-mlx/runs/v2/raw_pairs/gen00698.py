from pyspark.sql import functions as F

result = (
    df_transactions
    .select("txn_type", "merchant")
    .distinct()
    .orderBy("merchant", F.col("txn_type").desc())
    .limit(5)
)
