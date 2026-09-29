from pyspark.sql import functions as F

base = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN"]) & F.col("customer_id").isNull() & F.col("status").isNotNull())
)

accounts_stats = (
    base
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
)

result = (
    accounts_stats
    .filter(F.col("avg_balance") >= 51000)
    .select("opened_month", "avg_balance", "pct_closed")
)
