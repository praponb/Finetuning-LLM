from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(12, 19))
    .select("id", "paid_at", "order_id", F.lower("method").alias("method_lower"))
    .orderBy(F.col("order_id").desc())
)
