from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .groupBy(
        F.when(F.col("txn_type").isin(["FEE", "DEBIT"]), "tier_1").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
    .filter(F.col("shell_count") >= 34)
    .orderBy(F.col("txn_type_group").desc())
)
