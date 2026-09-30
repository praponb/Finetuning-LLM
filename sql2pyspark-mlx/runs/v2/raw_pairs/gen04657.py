from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
    .select("amount", "paid_at", "method", F.trim("method").alias("clean_method"))
    .orderBy(F.col("method").desc(), "paid_at")
)
