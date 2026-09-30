from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "is_flagged")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
    )
    .filter(F.col("pct_refund") > 37)
    .orderBy(F.col("pct_refund").desc(), "txn_month")
)
