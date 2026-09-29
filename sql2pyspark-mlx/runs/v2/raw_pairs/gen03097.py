from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .groupBy("status", F.to_date("paid_at").alias("paid_day"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
    .filter(F.col("pct_cash") < 69)
    .orderBy("status")
)
