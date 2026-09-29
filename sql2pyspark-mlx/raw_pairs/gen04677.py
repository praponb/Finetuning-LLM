from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1690, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
        F.date_trunc("month", "paid_at").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        ((F.unix_timestamp(F.max("paid_at")) - F.unix_timestamp(F.min("paid_at"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("method") == "promptpay", F.col("amount")).otherwise(0)).alias("promptpay_amount"),
    )
    .filter(F.col("amount_p95") < 1000000)
    .orderBy(F.col("amount_p95").desc_nulls_last())
)
