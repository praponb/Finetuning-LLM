from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1800, "HIGH").otherwise("LOW").alias("amount_tier"),
        F.date_trunc("MONTH", "paid_at").alias("activity_month"),
    )
    .agg(
        F.stddev_samp("amount").alias("amount_stddev"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_rows"),
        F.count(F.when(F.col("method") == "cash", 1)).alias("cash_rows"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
