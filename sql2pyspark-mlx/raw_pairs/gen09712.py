from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").isNull())
    .groupBy("status", "order_id")
    .agg(
        F.min("paid_at").alias("first_paid_at"),
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
    .orderBy(F.col("pct_refunded").desc())
)
