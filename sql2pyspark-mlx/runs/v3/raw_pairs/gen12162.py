from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2025-06-17")
    .groupBy("method")
    .pivot("status", ["REFUNDED", "FAILED"])
    .agg(F.max("amount"))
)
