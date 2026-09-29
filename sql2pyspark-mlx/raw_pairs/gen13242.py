from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_time") < "2023-05-01")
    .select("id", "txn_type", "merchant", "is_flagged")
    .orderBy(F.col("merchant").desc(), F.col("id").desc())
)
