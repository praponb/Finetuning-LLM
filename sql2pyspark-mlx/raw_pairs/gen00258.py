from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged") & (F.col("txn_type") != "CREDIT"))
    .select("id", "is_flagged", "txn_type", F.lower("txn_type").alias("txn_type_lower"))
)
