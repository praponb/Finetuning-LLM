from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.col("method").isin(["cash", "bank_transfer"]) & F.col("status").isin(["SUCCESS", "FAILED", "REFUNDED"]))
)

summary = (
    filtered_payments
    .groupBy("method")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    summary
    .select("method", "avg_amount")
    .orderBy(F.col("avg_amount").desc())
)
