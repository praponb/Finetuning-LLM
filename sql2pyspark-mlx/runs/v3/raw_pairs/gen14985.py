from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 65000, "high").when(F.col("balance") >= 15000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("min_opened_date").desc())
)
