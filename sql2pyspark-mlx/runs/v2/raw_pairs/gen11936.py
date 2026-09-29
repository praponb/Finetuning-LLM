from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id", "status")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
    )
)
