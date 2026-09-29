from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("amount") < 1900) & F.col("is_flagged"))
    .select("txn_time")
    .orderBy(F.col("txn_time").desc())
)
