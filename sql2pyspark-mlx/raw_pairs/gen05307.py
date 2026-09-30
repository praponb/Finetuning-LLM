from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee")
w2 = Window.partitionBy(F.date_trunc("month", "start_date"), "status").orderBy("monthly_fee", "id")

with_windows = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        F.rank().over(w).alias("monthly_fee_rank"),
        F.ntile(3).over(w2).alias("monthly_fee_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("monthly_fee_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_rank_tier"),
        F.when(F.col("monthly_fee_bucket") >= 8, "HIGH").otherwise("LOW").alias("monthly_fee_bucket_tier"),
        F.date_trunc("WEEK", "start_date").alias("week_start"),
    )
    .agg(
        F.count_if(F.col("plan") == "premium").alias("premium_events"),
        F.sum(F.when(F.col("plan") == "basic", F.col("monthly_fee_bucket")).otherwise(0)).alias("basic_monthly_fee_bucket"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.percentile_approx("monthly_fee_bucket", 0.9).alias("monthly_fee_bucket_p90"),
        F.percentile_approx("monthly_fee_rank", 0.5).alias("monthly_fee_rank_p50"),
    )
    .orderBy(F.col("week_start").asc_nulls_last())
)
