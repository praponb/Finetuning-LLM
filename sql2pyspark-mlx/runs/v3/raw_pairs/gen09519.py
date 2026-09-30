from pyspark.sql import functions as F

src = df_payments.filter(F.col("order_id").isNull())

payments_stats = (
    src
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = payments_stats.filter(F.col("avg_amount") < 800)
