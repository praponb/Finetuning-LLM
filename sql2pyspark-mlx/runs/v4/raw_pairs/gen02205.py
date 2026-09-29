from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", F.date_trunc("MONTH", "paid_at").alias("activity_month"))
    .agg(
        F.count_if(F.col("status") == "FAILED").alias("failed_events"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
    )
    .orderBy(F.col("activity_month").desc_nulls_first())
)
