from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([7, 40]) & (F.col("paid_at") >= "2025-03-11"))
    .select("id", "amount", F.round("amount", 0).alias("amount_rounded"))
)
