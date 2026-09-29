from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.month("txn_time").alias("txn_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("amount_count") > 1)
    .drop("amount_count")
    .orderBy(F.col("pct_refund").desc(), F.col("txn_month").desc())
    .limit(50)
)
