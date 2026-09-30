from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("paid_at") >= "2023-11-17") & (F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]) | (F.col("paid_at") >= "2025-03-17"))))
    .select("id", "method", "paid_at", F.to_date("paid_at").alias("paid_at_date"))
)
