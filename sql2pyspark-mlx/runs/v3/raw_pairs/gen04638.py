from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .groupBy("status", "account_type")
    .agg(
        F.countDistinct("balance").alias("unique_statuss"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("avg_balance") < 27)
    .select("status", "account_type", "unique_statuss", "avg_balance")
    .orderBy(F.col("avg_balance").desc(), "status", "account_type")
    .limit(5)
)
