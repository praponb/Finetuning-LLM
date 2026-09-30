from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "txn_type", "txn_time")
    .orderBy(F.col("txn_type").desc())
)
