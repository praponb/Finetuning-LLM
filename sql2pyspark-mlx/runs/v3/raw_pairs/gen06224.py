from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("txn_time", "txn_type", F.upper("txn_type").alias("txn_type_upper"))
    .orderBy(F.col("txn_time").desc())
)
