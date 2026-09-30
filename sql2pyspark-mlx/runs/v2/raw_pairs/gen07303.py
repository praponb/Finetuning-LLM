from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "card")
    .select("paid_at", "amount", F.substring("method", 1, 2).alias("method_prefix"))
)
