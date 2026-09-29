from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull() & ((F.col("balance") < 17000) | ~F.col("status").isin(["FROZEN", "OPEN"])))
    .groupBy(
        F.when(F.col("balance") >= 59000, "high").when(F.col("balance") >= 7000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.count("balance").alias("balance_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)
