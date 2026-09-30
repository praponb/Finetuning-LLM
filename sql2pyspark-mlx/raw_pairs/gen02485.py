from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type")
    .agg(
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
        F.count("balance").alias("balance_count"),
    )
    .filter(F.col("balance_count") >= 3)
    .drop("balance_count")
    .orderBy("credit_balance", "account_type")
    .limit(5)
)
