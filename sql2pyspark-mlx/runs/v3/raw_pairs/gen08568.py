from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 72000, "high").when(F.col("balance") >= 11000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.round(((100 * F.sum(F.when(F.col("account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .orderBy(F.col("open_count").desc())
)
