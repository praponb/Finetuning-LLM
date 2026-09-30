from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-01-01")
    .groupBy("method", F.month("paid_at").alias("paid_month"), "order_id")
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("method") == "card", F.col("amount")).otherwise(0)).alias("card_amount"),
        F.max("paid_at").alias("last_paid_at"),
    )
    .orderBy("amount_count")
)
