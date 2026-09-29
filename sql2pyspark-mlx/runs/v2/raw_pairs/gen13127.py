from pyspark.sql import functions as F

result = (
    df_transactions
    .select("account_id", "is_flagged", F.length("merchant").alias("merchant_length"))
    .orderBy(F.col("merchant_length").desc())
)
