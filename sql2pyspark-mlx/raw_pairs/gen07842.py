from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("running_sum_next_monthly_fee", "id")
w_grp = Window.orderBy("running_min_next_monthly_fee", "id")
w_hist = Window.orderBy("prev_first_known_start_date_dayofweek_whole")
w_ordered = Window.orderBy("prev_first_known_start_date_dayofweek_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("prev_first_known_start_date_dayofweek_whole_percent_rank", "id").rowsBetween(-5, -1)
w9 = Window.orderBy("next_running_avg_next_monthly_fee", "id").rowsBetween(-5, Window.currentRow)
w10 = Window.orderBy("prev_first_known_start_date_dayofweek_whole_2", "id").rowsBetween(-3, -1)

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-04-30"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
    )
)

scored = (
    valid
    .select(
        "id",
        "monthly_fee",
        F.lead("start_date_dayofweek").over(w_recent).alias("next_start_date_dayofweek"),
        F.lag("start_date_dayofweek").over(w_recent).alias("prev_start_date_dayofweek"),
        F.lead("monthly_fee").over(w_recent).alias("next_monthly_fee"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.first("start_date_dayofweek", ignorenulls=True).over(w2).alias("first_known_start_date_dayofweek"),
    )
)

with_windows = (
    scored
    .filter(F.col("row_seq").between(3, 7))
    .select(
        "id",
        "row_seq",
        "first_known_start_date_dayofweek",
        F.sum("next_monthly_fee").over(w3).alias("running_sum_next_monthly_fee"),
        F.count("next_monthly_fee").over(w3).alias("running_count_next_monthly_fee"),
        F.min("next_monthly_fee").over(w3).alias("running_min_next_monthly_fee"),
        F.avg("next_monthly_fee").over(w3).alias("running_avg_next_monthly_fee"),
    )
)

with_windows_subscriptions = (
    with_windows
    .select(
        "id",
        "first_known_start_date_dayofweek",
        "running_sum_next_monthly_fee",
        "running_count_next_monthly_fee",
        F.lead("running_avg_next_monthly_fee", 1).over(w4).alias("next_running_avg_next_monthly_fee"),
        F.lag("first_known_start_date_dayofweek", 1).over(w_grp).alias("prev_first_known_start_date_dayofweek"),
        F.ntile(3).over(w4).alias("running_sum_next_monthly_fee_bucket"),
    )
)

cleaned = (
    with_windows_subscriptions
    .filter(F.col("running_count_next_monthly_fee") >= 15)
    .select(
        "id",
        "first_known_start_date_dayofweek",
        "running_sum_next_monthly_fee",
        "next_running_avg_next_monthly_fee",
        F.ceil("prev_first_known_start_date_dayofweek").alias("prev_first_known_start_date_dayofweek_whole"),
        F.ceil("prev_first_known_start_date_dayofweek").alias("prev_first_known_start_date_dayofweek_whole_2"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "first_known_start_date_dayofweek",
        "running_sum_next_monthly_fee",
        "next_running_avg_next_monthly_fee",
        "prev_first_known_start_date_dayofweek_whole_2",
        F.percent_rank().over(w_hist).alias("prev_first_known_start_date_dayofweek_whole_percent_rank"),
        F.sum("prev_first_known_start_date_dayofweek_whole_2").over(w_ordered).alias("running_prev_first_known_start_date_dayofweek_whole_2"),
    )
)

enriched_subscriptions = (
    enriched
    .select(
        "*",
        F.avg("next_running_avg_next_monthly_fee").over(w8).alias("next_running_avg_next_monthly_fee_avg_5_prior"),
        F.avg("running_prev_first_known_start_date_dayofweek_whole_2").over(w9).alias("running_prev_first_known_start_date_dayofweek_whole_2_avg_5"),
        F.avg("running_prev_first_known_start_date_dayofweek_whole_2").over(w10).alias("running_prev_first_known_start_date_dayofweek_whole_2_avg_3_prior"),
    )
)

totals = (
    enriched_subscriptions
    .groupBy(
        F.when(F.col("first_known_start_date_dayofweek") >= 89, "LARGE").otherwise("SMALL").alias("first_known_start_date_dayofweek_tier"),
        F.when(F.col("running_prev_first_known_start_date_dayofweek_whole_2") >= 350, "PREMIUM").otherwise("STANDARD").alias("running_prev_first_known_start_date_dayofweek_whole_2_tier"),
    )
    .agg(
        F.min("running_prev_first_known_start_date_dayofweek_whole_2").alias("min_running_prev_first_known_start_date_dayofweek_whole_2"),
        F.percentile_approx("prev_first_known_start_date_dayofweek_whole_percent_rank", [0.25, 0.5, 0.75]).alias("prev_first_known_start_date_dayofweek_whole_percent_rank_quartiles"),
        F.percentile_approx("prev_first_known_start_date_dayofweek_whole_2", 0.9).alias("prev_first_known_start_date_dayofweek_whole_2_p90"),
    )
)

result = (
    totals
    .select(
        "first_known_start_date_dayofweek_tier",
        "running_prev_first_known_start_date_dayofweek_whole_2_tier",
        "min_running_prev_first_known_start_date_dayofweek_whole_2",
        "prev_first_known_start_date_dayofweek_whole_percent_rank_quartiles",
    )
    .orderBy(
        F.col("min_running_prev_first_known_start_date_dayofweek_whole_2").asc_nulls_first(),
    )
)
