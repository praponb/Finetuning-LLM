from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(~F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]) & (F.col("amount") <= 1430)))
    .select("status", "paid_at", "amount")
)
