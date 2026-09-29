from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(620, 990) & F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .select("status", "paid_at", F.lower("status").alias("status_lower"))
    .orderBy("status")
)
