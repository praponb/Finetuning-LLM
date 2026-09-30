from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "account_type",
        F.when(F.col("balance") >= 64000, "high").when(F.col("balance") >= 12000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.max("balance").alias("max_balance"),
        F.count("balance").alias("balance_count"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .orderBy("account_type")
)
