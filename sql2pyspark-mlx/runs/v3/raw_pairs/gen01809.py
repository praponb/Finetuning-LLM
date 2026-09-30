from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["paypal", "cash", "bank_transfer"]))
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.min("paid_at").alias("min_paid_at"),
        F.max("amount").alias("max_amount"),
    )
    .orderBy("amount_count")
)
