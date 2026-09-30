from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") < 1320) | F.col("order_id").isin([6, 15, 19]))
    .groupBy("status")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
    )
    .orderBy("pct_failed", "status")
    .limit(1)
)
