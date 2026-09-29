from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter((F.col("account_type") == "CREDIT") & (F.col("balance") <= 68000))
    .withColumn("is_closed", F.when(F.col("status") == "CLOSED", 1).otherwise(0))
)

accounts_stats = (
    valid_accounts
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
    )
)

result = accounts_stats.select("status", "pct_credit")
