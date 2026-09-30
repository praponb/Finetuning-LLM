from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .pivot("txn_type", ["DEBIT", "REFUND"])
    .agg(F.max("amount").alias("total"))
    .select("merchant", F.col("DEBIT").alias("debit"), F.col("REFUND").alias("refund"))
)
