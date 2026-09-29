from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "cash"]) & (F.to_date("paid_at") < "2023-12-16"))
    .groupBy("status", F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sum(F.when(F.col("status") == "FAILED", F.col("amount")).otherwise(0)).alias("failed_amount"),
    )
    .orderBy(F.col("cash_count").desc(), "status", "paid_hour")
    .limit(3)
)
