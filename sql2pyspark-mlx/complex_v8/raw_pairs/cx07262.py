from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product_count_median = Window.partitionBy("policy_product_count", "median_policy_product_count")
w_median_policy_product_count = Window.partitionBy("median_policy_product_count", "policy_product_count").orderBy("total_policy_count", "first_policy_start_date_ym", "policy_count", "policy_product_count").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_median_policy_product_count_2 = Window.partitionBy("median_policy_product_count", "policy_product_count").orderBy("total_policy_count", "first_policy_start_date_ym", "policy_count", "policy_product_count")
w_median_policy_product_count_3 = Window.partitionBy("median_policy_product_count", "policy_product_count").orderBy("total_policy_count", "first_policy_start_date_ym", "policy_count", "policy_product_count").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_count_next3_policy_co = Window.partitionBy("policy_count", "next3_policy_count")

filtered_holders = (
    df_policyholders
    .filter((F.col("risk_score") < 0.7) & (F.col("joined_on") >= F.lit("2024-09-04").cast("date")))
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        F.month(F.col("joined_on")).alias("joined_on_month"),
        F.lower(F.regexp_replace(F.trim(F.col("segment")), " ", "_")).alias("segment_slug"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("product").isin("HOME", "TRAVEL", "HEALTH"), 1)).alias("policy_product_count"),
    )
)

summarized_rows = (
    filtered_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.min("start_date").alias("first_policy_start_date"),
    )
)

profiled = (
    summarized_rows.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.first_policy_start_date",
    )
)

stats = (
    profiled
    .groupBy(
        F.date_format("first_policy_start_date", "yyyy-MM").alias("first_policy_start_date_ym"),
        "policy_count",
        "policy_product_count",
    )
    .agg(
        F.percentile_approx("policy_product_count", 0.5).alias("median_policy_product_count"),
        F.sum("policy_count").alias("total_policy_count"),
    )
)

ranked = (
    stats
    .select(
        "first_policy_start_date_ym",
        "policy_count",
        "policy_product_count",
        "median_policy_product_count",
        "total_policy_count",
        F.count(F.lit(1)).over(w_policy_product_count_median).alias("rows_in_policy_product_count_median_policy_produ"),
        F.nth_value("policy_product_count", 2).over(w_median_policy_product_count).alias("second_policy_product_count"),
        (F.col("total_policy_count") - F.lag("total_policy_count").over(w_median_policy_product_count_2)).alias("total_policy_count_change"),
        F.round(F.percent_rank().over(w_median_policy_product_count_2), 3).alias("total_policy_count_percent_rank"),
        F.lead("policy_count", 3).over(w_median_policy_product_count_2).alias("next3_policy_count"),
        F.sum("total_policy_count").over(w_median_policy_product_count_3).alias("running_total_policy_count"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.sum("running_total_policy_count").over(w_policy_count_next3_policy_co).alias("policy_count_next3_policy_count_sum_running_tota"),
        F.min("policy_product_count").over(w_policy_count_next3_policy_co).alias("policy_count_next3_policy_count_min_policy_produ"),
        F.round(F.avg("running_total_policy_count").over(w_policy_count_next3_policy_co), 2).alias("policy_count_next3_policy_count_avg_running_tota"),
        F.round(F.col("next3_policy_count") - F.avg("next3_policy_count").over(w_policy_count_next3_policy_co), 2).alias("next3_policy_count_vs_policy_count_next3_policy_"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.coalesce(F.col("next3_policy_count"), F.lit(0)).alias("next3_policy_count_filled"),
        (F.col("next3_policy_count") - F.col("policy_product_count")).alias("next3_policy_count_minus_policy_product_count"),
    )
)

result = (
    labeled
    .select(
        "first_policy_start_date_ym",
        "policy_count",
        "policy_product_count",
        "total_policy_count",
        "rows_in_policy_product_count_median_policy_produ",
        "total_policy_count_change",
        "total_policy_count_percent_rank",
        "next3_policy_count",
        "policy_count_next3_policy_count_min_policy_produ",
        "policy_count_next3_policy_count_avg_running_tota",
        "next3_policy_count_filled",
        "next3_policy_count_minus_policy_product_count",
        F.greatest(F.col("policy_product_count"), F.col("next3_policy_count")).alias("greatest_policy_product_count_next3_policy_count"),
    )
    .orderBy("policy_count_next3_policy_count_avg_running_tota")
)
