from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("month", "end_date"), "status").orderBy("start_date", "id")
w_running = Window.orderBy("last7_min_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("last7_min_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("week", "start_date")).orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_prev_monthly_fee")
w7 = Window.orderBy("last_known_prev_monthly_fee", "id")
w8 = Window.orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.orderBy("last7_count_monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.orderBy("running_prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("next_last7_count_monthly_fee").asc_nulls_first())
w12 = Window.orderBy("running_last7_max_monthly_fee", "id")
w13 = Window.partitionBy(F.date_trunc("week", "start_date"))
w14 = Window.orderBy("next_running_prev_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.orderBy(F.col("last3_avg_next_last7_count_monthly_fee").desc(), "id")

with_windows = (
    df_subscriptions
    .filter(F.month("end_date").isin([4, 6, 12]))
    .select(
        "*",
        F.min("monthly_fee").over(w).alias("last7_min_monthly_fee"),
        F.max("monthly_fee").over(w).alias("last7_max_monthly_fee"),
        F.count("monthly_fee").over(w).alias("last7_count_monthly_fee"),
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "start_date",
        "last7_min_monthly_fee",
        "last7_max_monthly_fee",
        "last7_count_monthly_fee",
        F.sum("prev_monthly_fee").over(w_running).alias("running_prev_monthly_fee"),
        F.last("prev_monthly_fee", ignorenulls=True).over(w4).alias("last_known_prev_monthly_fee"),
        F.sum("last7_max_monthly_fee").over(w5).alias("running_last7_max_monthly_fee"),
    )
)

scored_subscriptions = (
    scored
    .select(
        "*",
        F.rank().over(w6).alias("running_prev_monthly_fee_rank"),
        F.lead("last7_count_monthly_fee", 1).over(w7).alias("next_last7_count_monthly_fee"),
        F.avg("last7_max_monthly_fee").over(w8).alias("last7_max_monthly_fee_avg_6"),
    )
)

metrics = (
    scored_subscriptions
    .withColumn("last3_avg_next_last7_count_monthly_fee", F.avg("next_last7_count_monthly_fee").over(w_part))
    .withColumn("last3_sum_next_last7_count_monthly_fee", F.sum("next_last7_count_monthly_fee").over(w_part))
    .withColumn("last3_min_next_last7_count_monthly_fee", F.min("next_last7_count_monthly_fee").over(w_part))
    .withColumn("cumulative_last7_max_monthly_fee_avg_6", F.sum("last7_max_monthly_fee_avg_6").over(w_hist))
)

base = (
    metrics
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-03-28")))
    .withColumn("last7_count_monthly_fee_minus_cumulative_last7_max_monthly_fee_avg_6", (F.col("last7_count_monthly_fee") - F.col("cumulative_last7_max_monthly_fee_avg_6")))
    .withColumn("running_last7_max_monthly_fee_rounded", F.round("running_last7_max_monthly_fee", 0))
)

metrics_subscriptions = (
    base
    .select(
        "id",
        "start_date",
        "last7_min_monthly_fee",
        "last7_count_monthly_fee",
        "running_prev_monthly_fee_rank",
        "next_last7_count_monthly_fee",
        "last3_avg_next_last7_count_monthly_fee",
        "last3_min_next_last7_count_monthly_fee",
        "cumulative_last7_max_monthly_fee_avg_6",
        "running_last7_max_monthly_fee_rounded",
        F.rank().over(w11).alias("next_last7_count_monthly_fee_rank"),
        F.lead("running_prev_monthly_fee", 2).over(w12).alias("next_running_prev_monthly_fee"),
        F.stddev_samp("running_last7_max_monthly_fee_rounded").over(w13).alias("start_date_period_stddev_samp_running_last7_max_monthly_fee_rounded"),
        F.round((F.col("last7_max_monthly_fee_avg_6") / F.sum("last7_max_monthly_fee_avg_6").over(w13)), 4).alias("last7_max_monthly_fee_avg_6_share"),
    )
)

filtered = (
    metrics_subscriptions
    .filter(F.col("next_last7_count_monthly_fee_rank") <= 5)
    .select(
        "id",
        "start_date",
        "last7_count_monthly_fee",
        "running_prev_monthly_fee_rank",
        "next_last7_count_monthly_fee",
        "last3_avg_next_last7_count_monthly_fee",
        "last3_min_next_last7_count_monthly_fee",
        "cumulative_last7_max_monthly_fee_avg_6",
        "next_last7_count_monthly_fee_rank",
        "next_running_prev_monthly_fee",
        "last7_max_monthly_fee_avg_6_share",
        F.col("last7_max_monthly_fee_avg_6_share").cast("int").alias("last7_max_monthly_fee_avg_6_share_int"),
    )
)

result = (
    filtered
    .withColumn("running_max_last7_max_monthly_fee_avg_6_share_int", F.max("last7_max_monthly_fee_avg_6_share_int").over(w14))
    .withColumn("running_count_last7_max_monthly_fee_avg_6_share_int", F.count("last7_max_monthly_fee_avg_6_share_int").over(w14))
    .withColumn("running_avg_last7_max_monthly_fee_avg_6_share_int", F.avg("last7_max_monthly_fee_avg_6_share_int").over(w14))
    .withColumn("last3_avg_next_last7_count_monthly_fee_bucket", F.ntile(5).over(w15))
)
