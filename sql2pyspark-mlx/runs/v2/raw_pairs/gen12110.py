from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.when(F.col("balance") >= 85000, "high").when(F.col("balance") >= 13000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_closed"),
    )
)
