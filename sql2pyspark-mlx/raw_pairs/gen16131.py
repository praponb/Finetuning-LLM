from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-10-01")
    .select("id", "order_id", F.lower("method").alias("method_lower"))
)
