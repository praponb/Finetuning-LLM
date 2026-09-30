from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("balance") > 43000) & F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]) & (F.col("opened_date") < "2025-02-01"))
    .withColumn("clean_status", F.trim("status"))
)

accounts_stats = (
    base
    .groupBy(
        "status",
        F.when(F.col("balance") >= 59000, "high").when(F.col("balance") >= 29000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.avg("balance").alias("avg_balance"))
)

result = (
    accounts_stats
    .filter(F.col("avg_balance") < 18000)
    .select("status", "balance_bucket", "avg_balance")
    .orderBy(F.col("status").desc())
)
