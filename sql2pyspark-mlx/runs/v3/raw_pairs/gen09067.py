from pyspark.sql import functions as F

base = (
    df_payments
    .filter(~F.col("method").isin(["bank_transfer", "promptpay"]) & (F.to_date("paid_at") >= "2023-07-01") & F.col("order_id").isNotNull())
)

summary = (
    base
    .groupBy(
        F.when(F.col("amount") >= 1770, "high").when(F.col("amount") >= 680, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("*").alias("payment_count"))
)

result = summary.select("amount_bucket", "payment_count")
