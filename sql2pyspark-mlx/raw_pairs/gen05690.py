from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("status") == "REFUNDED") & (F.col("order_id") == 6)) | F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
    .agg(F.max("paid_at").alias("latest_paid_at"))
    .orderBy("latest_paid_at")
)
