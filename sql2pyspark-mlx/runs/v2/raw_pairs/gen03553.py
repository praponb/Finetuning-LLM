from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 1600)
    .groupBy(F.hour("txn_time").alias("txn_hour"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.sum("amount").alias("total_amount"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
    .filter(F.col("shell_count") < 70)
    .drop("shell_count")
)
