from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("paid_at") < "2024-02-09"))
    .withColumn("paid_at_date", F.to_date("paid_at"))
)

summary = (
    filtered_payments
    .groupBy(F.quarter("paid_at_date").alias("paid_quarter"), "method")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    summary
    .filter(F.col("promptpay_count") > 34)
    .orderBy(F.col("method").desc(), "paid_quarter")
    .limit(1)
)
