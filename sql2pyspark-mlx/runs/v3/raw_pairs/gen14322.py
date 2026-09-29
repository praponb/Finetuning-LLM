from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") < "2025-05-01") & F.hour("paid_at").between(10, 13) & (F.col("order_id") == 12))
    .agg(F.min("amount").alias("min_amount"))
)
