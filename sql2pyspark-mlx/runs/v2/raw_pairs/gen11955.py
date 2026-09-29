from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2025-01-01") | F.col("method").isin(["card", "cash"]))
    .select("id", "amount", F.lower("status").alias("status_lower"))
)
