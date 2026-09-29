from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", "status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        F.countDistinct("amount").alias("unique_orders"),
    )
    .orderBy(F.col("pct_refunded").desc())
)
