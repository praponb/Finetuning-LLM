from pyspark.sql import functions as F

base = df_accounts.filter(F.col("account_type").isin(["LOAN", "CHECKING"]))

accounts_stats = (
    base
    .groupBy(
        "status",
        F.when(F.col("balance") >= 70000, "high").when(F.col("balance") >= 7000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    accounts_stats
    .select("status", "balance_bucket", "min_balance")
    .orderBy(F.col("balance_bucket").desc())
)
