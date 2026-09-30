from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("quarter_start", F.date_trunc("QUARTER", "paid_at"))
    .withColumn("activity_month", F.date_trunc("MONTH", "paid_at"))
    .withColumn("week_start", F.date_trunc("WEEK", "paid_at"))
    .rollup("quarter_start", "activity_month", "week_start")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.min("paid_at").alias("started_at"),
        F.count(F.when(F.col("method") == "bank_transfer", 1)).alias("bank_transfer_count"),
        F.grouping("quarter_start").alias("grouping_id"),
    )
)
