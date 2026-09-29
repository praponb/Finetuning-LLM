from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("monthly_fee")
w3 = Window.orderBy("days_start_date_to_end_date", "id")
w_hist = Window.orderBy("monthly_fee_cume_dist_times_greatest_monthly_fee_cumulative_days_start_date_to_end_date", "id")
w5 = Window.orderBy(F.col("days_start_date_to_end_date").desc_nulls_last())

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & (F.col("status") == "CANCELLED"))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

valid = (
    filtered
    .filter(F.col("start_date") >= F.lit("2023-09-03").cast("date"))
    .select("*", F.last_day("start_date").alias("start_date_month_end"))
)

enriched = (
    valid
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "days_start_date_to_end_date",
        "start_date_month_end",
        F.sum("days_start_date_to_end_date").over(w).alias("cumulative_days_start_date_to_end_date"),
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist"),
        F.ntile(5).over(w3).alias("days_start_date_to_end_date_bucket"),
    )
)

base = (
    enriched
    .filter(F.year("start_date_month_end") == 2025)
    .withColumn("greatest_monthly_fee_cumulative_days_start_date_to_end_date", F.greatest("monthly_fee", "cumulative_days_start_date_to_end_date"))
)

filtered_subscriptions = (
    base
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-07-05")))
    .select(
        "*",
        (F.col("monthly_fee_cume_dist") * F.col("greatest_monthly_fee_cumulative_days_start_date_to_end_date")).alias("monthly_fee_cume_dist_times_greatest_monthly_fee_cumulative_days_start_date_to_end_date"),
    )
)

metrics = (
    filtered_subscriptions
    .filter(F.col("start_date_month_end") < "2024-01-01")
    .select(
        "*",
        F.lead("monthly_fee_cume_dist").over(w_hist).alias("next_monthly_fee_cume_dist"),
        F.lag("monthly_fee_cume_dist").over(w_hist).alias("prev_monthly_fee_cume_dist"),
        F.lag("greatest_monthly_fee_cumulative_days_start_date_to_end_date").over(w_hist).alias("prev_greatest_monthly_fee_cumulative_days_start_date_to_end_date"),
        F.lead("greatest_monthly_fee_cumulative_days_start_date_to_end_date").over(w_hist).alias("next_greatest_monthly_fee_cumulative_days_start_date_to_end_date"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.dense_rank().over(w5).alias("days_start_date_to_end_date_dense_rank"),
    )
)

base_subscriptions = (
    metrics
    .filter(F.col("start_date") >= F.lit("2023-05-06").cast("date"))
    .withColumn("event_seq_per_next_greatest_monthly_fee_cumulative_days_start_date_to_end_date", (F.col("event_seq") / F.nullif("next_greatest_monthly_fee_cumulative_days_start_date_to_end_date", F.lit(0))))
    .withColumn("monthly_fee_cume_dist_per_cumulative_days_start_date_to_end_date", (F.col("monthly_fee_cume_dist") / F.nullif("cumulative_days_start_date_to_end_date", F.lit(0))))
)

result = (
    base_subscriptions
    .groupBy(
        F.when(F.col("event_seq_per_next_greatest_monthly_fee_cumulative_days_start_date_to_end_date") >= 66, "HIGH").otherwise("LOW").alias("event_seq_per_next_greatest_monthly_fee_cumulative_days_start_date_to_end_date_tier"),
    )
    .agg(
        F.stddev_samp("monthly_fee").alias("monthly_fee_stddev"),
        F.percentile_approx("prev_greatest_monthly_fee_cumulative_days_start_date_to_end_date", 0.5).alias("prev_greatest_monthly_fee_cumulative_days_start_date_to_end_date_p50"),
        F.min("greatest_monthly_fee_cumulative_days_start_date_to_end_date").alias("min_greatest_monthly_fee_cumulative_days_start_date_to_end_date"),
    )
    .orderBy(F.col("monthly_fee_stddev").desc())
)
