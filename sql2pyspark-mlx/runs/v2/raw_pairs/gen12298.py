from pyspark.sql import functions as F

recent_accounts = (
    df_accounts
    .filter(F.col("account_type").isin(["SAVINGS", "CREDIT", "LOAN"]) & F.col("opened_date").between("2024-02-01", "2025-03-01"))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

accounts_stats = (
    recent_accounts
    .groupBy("status")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)

result = accounts_stats.orderBy("status")
