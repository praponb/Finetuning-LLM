from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .orderBy("pct_refunded")
)
