from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee") >= 261, "HIGH").otherwise("LOW").alias("monthly_fee_tier"),
        F.date_trunc("quarter", "start_date").alias("quarter_start"),
    )
    .agg(
        F.count_if(F.col("status") == "PAUSED").alias("paused_events"),
        F.sum("monthly_fee").alias("total_monthly_fee"),
    )
    .orderBy("monthly_fee_tier")
)
