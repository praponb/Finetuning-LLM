from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "FAILED") & F.col("order_id").isNull())
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy(F.col("first_paid_at").desc())
)
