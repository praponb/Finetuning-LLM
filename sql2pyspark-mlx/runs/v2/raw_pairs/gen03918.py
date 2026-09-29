from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "REFUNDED") & (F.col("method") != "bank_transfer"))
    .select("id", "method", "order_id", F.lower("status").alias("status_lower"))
)
