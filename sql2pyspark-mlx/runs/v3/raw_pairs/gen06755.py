from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("method").isNotNull() & F.col("amount").between(870, 1500) & (F.col("method") != "bank_transfer"))
)

payments_stats = (
    recent_payments
    .groupBy("status")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    payments_stats
    .filter(F.col("avg_amount") > 66)
    .select("status", "avg_amount")
    .orderBy(F.col("status").desc())
    .limit(10)
)
