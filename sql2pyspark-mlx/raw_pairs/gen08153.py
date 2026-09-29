from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(~(~F.col("status").isin(["FROZEN", "CLOSED"]) & F.col("balance").between(29000, 52000)))
    .groupBy(
        F.when(F.col("balance") >= 83000, "high").when(F.col("balance") >= 8000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy(F.col("avg_balance").desc(), F.col("balance_bucket").desc())
)
