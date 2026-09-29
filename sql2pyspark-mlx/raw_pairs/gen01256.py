from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id", "status")
w3 = Window.partitionBy("status", "plan")
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_share").desc(), "id")
w5 = Window.orderBy(F.col("row_seq").desc())
w6 = Window.orderBy("row_seq", "id").rowsBetween(-3, -1)
w7 = Window.orderBy("monthly_fee_share_bucket", "id")
w8 = Window.orderBy("next_monthly_fee_share_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("row_seq_avg_3_prior_minus_next_monthly_fee_share", "id").rowsBetween(-2, Window.currentRow)

with_windows = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "status",
        F.lead("monthly_fee").over(w_hist).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w_hist).alias("prev_monthly_fee"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "next_monthly_fee",
        "row_seq",
        F.round((F.col("next_monthly_fee") / F.sum("next_monthly_fee").over(w3)), 4).alias("next_monthly_fee_share"),
        F.ntile(4).over(w4).alias("monthly_fee_share_bucket"),
    )
)

scored_subscriptions = (
    scored
    .filter(F.col("monthly_fee_share_bucket") <= 4)
    .withColumn("row_seq_rank", F.rank().over(w5))
    .withColumn("row_seq_avg_3_prior", F.avg("row_seq").over(w6))
    .withColumn("next_next_monthly_fee_share", F.lead("next_monthly_fee_share", 2).over(w7))
)

valid = (
    scored_subscriptions
    .filter(F.col("next_next_monthly_fee_share") <= 0.6)
    .select(
        "*",
        (F.col("row_seq_avg_3_prior") - F.col("next_monthly_fee_share")).alias("row_seq_avg_3_prior_minus_next_monthly_fee_share"),
        F.round("next_monthly_fee_share", 0).alias("next_monthly_fee_share_rounded"),
    )
)

scored_subscriptions_2 = (
    valid
    .select(
        "id",
        "row_seq",
        "next_monthly_fee_share",
        "row_seq_rank",
        "row_seq_avg_3_prior",
        "next_next_monthly_fee_share",
        "row_seq_avg_3_prior_minus_next_monthly_fee_share",
        "next_monthly_fee_share_rounded",
        F.max("row_seq_avg_3_prior").over(w8).alias("running_max_row_seq_avg_3_prior"),
        F.count("row_seq_avg_3_prior").over(w8).alias("running_count_row_seq_avg_3_prior"),
        F.avg("row_seq_avg_3_prior").over(w8).alias("running_avg_row_seq_avg_3_prior"),
        F.min("row_seq_avg_3_prior").over(w8).alias("running_min_row_seq_avg_3_prior"),
        F.avg("next_monthly_fee").over(w9).alias("next_monthly_fee_avg_2"),
    )
)

result = (
    scored_subscriptions_2
    .groupBy(
        F.when(F.col("row_seq_avg_3_prior_minus_next_monthly_fee_share") >= 72, "HIGH").otherwise("LOW").alias("row_seq_avg_3_prior_minus_next_monthly_fee_share_tier"),
        F.when(F.col("running_max_row_seq_avg_3_prior") >= 6.8, "PREMIUM").otherwise("STANDARD").alias("running_max_row_seq_avg_3_prior_tier"),
    )
    .agg(
        F.percentile_approx("next_monthly_fee_share_rounded", [0.25, 0.5, 0.75]).alias("next_monthly_fee_share_rounded_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("row_seq_avg_3_prior_minus_next_monthly_fee_share", 0.95).alias("row_seq_avg_3_prior_minus_next_monthly_fee_share_p95"),
        F.count("*").alias("event_count"),
    )
)
