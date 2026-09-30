from pyspark.sql import functions as F

valid_accounts = (
    df_accounts
    .filter(F.col("status").isin(["OPEN", "CLOSED"]) & F.col("account_type").isin(["SAVINGS", "CHECKING"]))
)

balance_bucket_summary = (
    valid_accounts
    .groupBy(
        F.when(F.col("balance") >= 68000, "high").when(F.col("balance") >= 30000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.max("balance").alias("max_balance"),
    )
)

result = (
    balance_bucket_summary
    .select("balance_bucket", "total_count", "max_balance")
    .orderBy(F.col("max_balance").desc(), "balance_bucket")
    .limit(20)
)
