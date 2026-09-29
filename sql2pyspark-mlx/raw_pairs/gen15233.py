from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method") == "cash")
    .select("amount", "method", "paid_at", F.round("amount", 0).alias("amount_rounded"))
)
