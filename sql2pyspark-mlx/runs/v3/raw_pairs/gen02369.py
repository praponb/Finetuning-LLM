from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") == "SUCCESS") & (F.col("paid_at") < "2023-05-08"))
    .select("paid_at", "order_id", "amount", F.lower("method").alias("method_lower"))
    .orderBy(F.col("paid_at").desc())
)
