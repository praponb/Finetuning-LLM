from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("order_id") == 8) & (F.to_date("paid_at") >= "2025-05-01")) | (F.col("status") == "SUCCESS"))
    .select("status", F.upper("status").alias("status_upper"))
    .orderBy(F.col("status_upper").desc(), F.col("status").desc())
)
