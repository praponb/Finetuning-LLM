from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 16, 32]) & F.col("status").isin(["FROZEN", "CLOSED"]))
)

summary = (
    base
    .groupBy(
        F.when(F.col("balance") >= 59000, "high").when(F.col("balance") >= 27000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = (
    summary
    .filter(F.col("avg_balance") > 33)
    .select("balance_bucket", "avg_balance")
    .orderBy(F.col("avg_balance").desc(), F.col("balance_bucket").desc())
    .limit(5)
)
