from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("account_type", "customer_id")
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
        F.count("balance").alias("balance_count"),
    )
    .orderBy(F.col("customer_id").desc())
)
