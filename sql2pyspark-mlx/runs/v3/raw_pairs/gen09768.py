from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "tier_1").otherwise("other").alias("status_group"),
        F.when(F.col("balance") >= 82000, "high").when(F.col("balance") >= 11000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .orderBy("balance_bucket")
)
