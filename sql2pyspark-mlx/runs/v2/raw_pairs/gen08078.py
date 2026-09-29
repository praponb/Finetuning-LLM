from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_type") != "CREDIT") & F.col("account_id").isin([27, 36]) & (F.col("txn_type") == "CREDIT"))
    .groupBy("is_flagged", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
        F.countDistinct("amount").alias("unique_accounts"),
    )
    .filter(F.col("unique_accounts") >= 5)
)
