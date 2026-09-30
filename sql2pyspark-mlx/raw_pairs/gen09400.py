from pyspark.sql import functions as F

base = (
    df_transactions
    .filter(F.col("is_flagged") | (F.col("txn_time") < "2024-05-01"))
    .withColumn("txn_time_period", F.date_trunc("YEAR", "txn_time"))
)

txn_month_summary = (
    base
    .groupBy(
        F.month("txn_time").alias("txn_month"),
        F.when(F.col("amount") >= 4400, "high").when(F.col("amount") >= 1400, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
)

result = (
    txn_month_summary
    .select("txn_month", "amount_bucket", "max_amount", "avg_amount")
    .orderBy(F.col("avg_amount").desc(), "txn_month", "amount_bucket")
    .limit(5)
)
