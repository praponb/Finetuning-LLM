from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance") < 56000)
    .groupBy(F.month("opened_date").alias("opened_month"), "status")
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .filter(F.col("open_count") >= 3)
    .orderBy(F.col("avg_balance").desc(), "opened_month")
)
