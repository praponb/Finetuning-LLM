from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("amount") <= 1360))
    .groupBy("order_id", "status")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.round(((100 * F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bank_transfer"),
    )
    .filter(F.col("pct_bank_transfer") >= 31)
    .orderBy(F.col("status").desc())
)
