from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull() & (F.col("order_id") == 25))
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
    )
    .filter(F.col("pct_failed") >= 56)
    .orderBy("method")
    .limit(5)
)
