from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", F.month("paid_at").alias("paid_month"))
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
)
