from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") != "DEBIT")
    .groupBy("account_id", "merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", F.col("amount")).otherwise(0)).alias("shell_amount"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") < 4000)
    .select("account_id", "merchant", "shell_amount", "max_amount")
)
