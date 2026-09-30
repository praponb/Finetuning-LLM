from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .select("order_id", "status", "paid_at", F.upper("method").alias("method_upper"))
)
