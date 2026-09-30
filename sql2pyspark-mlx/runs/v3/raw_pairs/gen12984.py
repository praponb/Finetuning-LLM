from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2023-03-18") & (F.col("order_id") == 12))
    .groupBy("status", "method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy(F.col("status").desc())
)
