from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("QUARTER", "end_date").alias("quarter_start"),
        F.when(F.col("monthly_fee") >= 235, "LARGE").otherwise("SMALL").alias("monthly_fee_tier"),
    )
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.count_if(F.col("status") == "ACTIVE").alias("active_events"),
    )
    .filter(F.col("enterprise_events") < 1000)
    .orderBy("quarter_start")
)
