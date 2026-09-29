from pyspark.sql import functions as F

base = (
    df_payments
    .filter(((F.col("order_id") == 4) & F.col("order_id").isNull()) | (F.col("status") == "SUCCESS"))
    .withColumn("days_since_paid_at", F.datediff(F.current_date(), "paid_at"))
)

payments_stats = (
    base
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "method")
    .agg(
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
        F.count("*").alias("num_payments"),
    )
)

result = (
    payments_stats
    .filter(F.col("success_count") >= 8)
    .select("paid_quarter", "method", "num_payments")
    .orderBy(F.col("num_payments").desc(), "method")
)
