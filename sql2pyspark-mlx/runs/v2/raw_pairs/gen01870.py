from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 16, 18]) & (F.col("opened_date") < "2023-08-01") & F.col("customer_id").isin([7, 26]))
    .groupBy(
        "customer_id",
        "account_type",
        F.when(F.col("balance") >= 67000, "high").when(F.col("balance") >= 27000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.avg("balance").alias("avg_balance"))
    .orderBy(F.col("customer_id").desc(), "account_type", "balance_bucket")
    .limit(20)
)
