from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "promptpay") & (F.col("method").isin(["bank_transfer", "paypal"]) | (F.col("amount") <= 570)))
    .groupBy("method", F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
        F.avg("amount").alias("avg_amount"),
        F.countDistinct("amount").alias("unique_orders"),
    )
)
