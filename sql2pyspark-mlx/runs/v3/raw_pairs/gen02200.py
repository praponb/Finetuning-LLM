from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select("id", "account_id", F.col("amount").cast("int").alias("amount_int"))
    .orderBy(F.col("id").desc(), F.col("amount_int").desc())
)
