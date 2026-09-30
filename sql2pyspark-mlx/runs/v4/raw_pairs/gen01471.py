from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") < 2100)
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
    )
)
