from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "card") | F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
        F.avg("amount").alias("avg_amount"),
        F.count("amount").alias("amount_count"),
    )
)
