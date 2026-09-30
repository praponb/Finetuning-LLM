from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") != "paypal")
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"), "method")
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("success_count") < 5)
    .select("paid_month", "method", "success_count", "amount_range")
    .orderBy("success_count")
)
