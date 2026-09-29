from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~(F.col("order_id").isNotNull() & (F.col("paid_at") >= "2024-04-01")))
)
