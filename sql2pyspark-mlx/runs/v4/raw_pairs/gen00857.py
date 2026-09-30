from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("paid_at") < "2024-09-01")
    .withColumn("method_status_key", F.concat_ws(",", "method", "status"))
)

payments_stats = (
    filtered_payments
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("method") == "promptpay", 1)).alias("promptpay_count"),
        F.count("*").alias("row_count"),
    )
)

result = (
    payments_stats
    .filter(F.col("promptpay_count") < 5)
    .select("status", "promptpay_count", "row_count")
)
