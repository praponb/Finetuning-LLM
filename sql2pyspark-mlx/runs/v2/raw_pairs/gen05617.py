from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("txn_type", F.date_trunc("MONTH", "txn_time").alias("txn_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .filter(F.col("pct_credit") >= 53)
)
