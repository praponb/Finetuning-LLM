from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("status") == "REFUNDED") & (F.col("paid_at") < "2023-09-01") & (F.col("amount") < 1010))
    .withColumn("paid_at_date", F.to_date("paid_at"))
)

payments_stats = (
    base
    .groupBy("method")
    .agg(F.count("*").alias("payment_count"))
)

result = (
    payments_stats
    .filter(F.col("payment_count") > 4)
    .select("method", "payment_count")
    .orderBy(F.col("payment_count").desc(), "method")
    .limit(50)
)
