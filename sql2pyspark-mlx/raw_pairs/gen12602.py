from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-02-01") & (F.col("paid_at") < "2023-05-01") & F.col("order_id").isNotNull())
    .select("method", "paid_at", F.substring("method", 1, 4).alias("method_prefix"))
)
