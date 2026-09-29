from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type")
    .pivot("merchant", ["Shell", "Grab"])
    .agg(F.sum("amount"))
    .select("txn_type", F.col("Shell").alias("shell"), F.col("Grab").alias("grab"))
)
