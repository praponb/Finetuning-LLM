from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "txn_time", F.length("merchant").alias("merchant_length"))
    .orderBy(F.col("id").desc(), F.col("merchant_length").desc())
    .limit(20)
)
