from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("account_id").isin([6, 10, 36, 39]))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
)

transactions_stats = (
    base
    .groupBy(F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    transactions_stats
    .filter(F.col("avg_amount") > 44)
    .orderBy("txn_month")
)
