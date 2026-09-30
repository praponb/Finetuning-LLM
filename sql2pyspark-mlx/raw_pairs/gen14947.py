from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]) & (F.col("amount") <= 810)))
    .select("paid_at")
    .orderBy("paid_at")
)
