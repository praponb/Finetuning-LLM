from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select("id", "amount", F.length("txn_type").alias("txn_type_length"))
)
