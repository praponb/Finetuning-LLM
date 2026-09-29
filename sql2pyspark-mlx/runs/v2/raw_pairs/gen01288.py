from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([12, 18, 21, 40]))
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"), "method")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
        F.min("amount").alias("min_amount"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .filter(F.col("failed_amount") >= 78)
    .drop("failed_amount")
    .orderBy(F.col("paid_month").desc())
)
