from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("opened_date").between("2023-07-12", "2024-07-01") & (F.col("balance") > 74000)) | F.col("balance").between(23000, 36000))
    .groupBy(
        "account_type",
        F.when(F.col("balance") >= 73000, "high").when(F.col("balance") >= 36000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum("balance").alias("total_balance"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy("avg_balance", "account_type", "balance_bucket")
    .limit(5)
)
