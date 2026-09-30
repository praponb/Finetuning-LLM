from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") < "2023-06-29") & (F.col("method") == "promptpay"))
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
    .filter(F.col("paypal_count") >= 39)
    .orderBy("paid_month")
)
