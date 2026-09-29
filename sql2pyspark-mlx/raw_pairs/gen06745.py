from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2025-08-01") & (F.col("status").isin(["REFUNDED", "FAILED", "SUCCESS"]) | F.col("order_id").isNotNull()))
)
