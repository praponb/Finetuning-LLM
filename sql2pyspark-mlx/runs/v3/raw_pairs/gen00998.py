from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") < 460)
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
)
