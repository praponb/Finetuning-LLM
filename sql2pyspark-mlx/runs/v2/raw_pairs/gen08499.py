from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(550, 560) & (F.col("order_id") == 23))
    .groupBy("status")
    .agg(F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"))
    .filter(F.col("refunded_count") < 1)
    .orderBy("status", "refunded_count")
    .limit(20)
)
