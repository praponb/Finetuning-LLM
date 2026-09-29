from pyspark.sql import functions as F

result = (
    df_payments
    .select("paid_at", "amount", F.round("amount", 0).alias("amount_rounded"))
    .orderBy("amount", "amount_rounded")
)
