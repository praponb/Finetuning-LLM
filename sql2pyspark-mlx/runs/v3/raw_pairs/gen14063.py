from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("pct_failed") < 32)
    .orderBy("method")
)
