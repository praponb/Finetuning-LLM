from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter(F.col("is_flagged") | F.col("is_flagged"))
    .withColumn("txn_time_date", F.to_date("txn_time"))
)

summary = (
    recent_transactions
    .groupBy(
        F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 1700, "mid").otherwise("low").alias("amount_bucket"),
        F.year("txn_time_date").alias("txn_year"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    summary
    .filter(F.col("avg_amount") >= 49)
    .select("amount_bucket", "txn_year", "avg_amount")
)
