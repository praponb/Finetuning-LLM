from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(1700, 2100) & (F.col("txn_time") < "2024-03-03") & F.hour("txn_time").between(6, 13))
    .groupBy("merchant", "txn_type")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.sum(F.when(F.col("merchant") == "Shell", F.col("amount")).otherwise(0)).alias("shell_amount"),
    )
    .filter(F.col("shell_amount") < 70)
    .drop("shell_amount")
)
