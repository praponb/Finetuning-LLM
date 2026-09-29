from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.to_date("paid_at") >= "2025-10-01") & F.col("method").isin(["bank_transfer", "paypal", "card"]))
    .groupBy(F.year("paid_at").alias("paid_year"), "status")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
    )
)
