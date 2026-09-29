from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([8, 9, 14, 32]) & F.col("amount").isNotNull() & F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .groupBy(F.quarter("paid_at").alias("paid_quarter"), "method")
    .agg(
        F.count("amount").alias("amount_count"),
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
    )
    .orderBy("amount_count")
)
