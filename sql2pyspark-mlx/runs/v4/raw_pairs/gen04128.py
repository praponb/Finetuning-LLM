from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("activity_month").orderBy(F.col("cancelled_total_monthly_fee").desc())

result = (
    df_subscriptions
    .groupBy(
        F.date_trunc("month", "end_date").alias("activity_month"),
        F.date_trunc("week", "start_date").alias("week_start"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee"))).alias("cancelled_total_monthly_fee"),
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
    )
    .withColumn("cancelled_total_monthly_fee_percent_rank", F.percent_rank().over(w))
    .orderBy(F.col("cancelled_count").desc())
)
