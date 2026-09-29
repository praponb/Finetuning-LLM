from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1060)
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
)
