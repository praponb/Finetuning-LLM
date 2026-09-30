from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 210, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
        F.date_trunc("QUARTER", "start_date").alias("quarter_start"),
        "status",
    )
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.sum(F.when(F.col("status") == "PAUSED", F.col("monthly_fee")).otherwise(0)).alias("paused_monthly_fee"),
    )
)
