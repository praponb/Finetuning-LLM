from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.to_date("paid_at") < "2025-07-01") & (F.col("method") == "cash")) | (F.col("amount") <= 1310))
    .groupBy("method", F.month("paid_at").alias("paid_month"))
    .agg(
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
    .filter(F.col("success_count") < 2)
    .orderBy("success_count", "method")
)
