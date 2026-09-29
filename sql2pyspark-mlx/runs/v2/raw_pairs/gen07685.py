from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0))) / F.count("*")), 1).alias("pct_promptpay"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
    .orderBy(F.col("success_count").desc())
)
