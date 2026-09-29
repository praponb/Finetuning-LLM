from pyspark.sql import functions as F

base = df_payments.filter((F.col("paid_at") >= "2025-05-01") & F.col("amount").isNull())

payments_stats = (
    base
    .groupBy("status")
    .agg(F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"))
)

result = payments_stats.filter(F.col("card_count") > 39)
