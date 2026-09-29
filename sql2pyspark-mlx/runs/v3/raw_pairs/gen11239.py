from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        "status",
        F.when(F.col("balance") >= 72000, "high").when(F.col("balance") >= 19000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.count("*").alias("cnt"),
    )
    .orderBy("status", "balance_bucket")
)
