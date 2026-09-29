from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") > 1500)
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"), "txn_type")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fee"),
    )
    .filter(F.col("pct_fee") > 41)
    .select("txn_month", "txn_type", "pct_fee")
)
