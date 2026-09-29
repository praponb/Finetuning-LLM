from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_end_date_span_days = Window.partitionBy("status").orderBy(F.col("end_date_span_days").desc(), "start_date_month", "status")
w_status_row_count = Window.partitionBy("status", "row_count")
w_row_count_status_by_end_date = Window.partitionBy("row_count", "status").orderBy("end_date_span_days", "start_date_month", "status")
w_all_by_prev_min_premium = Window.orderBy(F.col("prev_min_premium").desc(), "start_date_month", "status")
w_end_date_span_days_quintile = Window.partitionBy("end_date_span_days_quintile", F.month("start_date_month"))
w_row_count_status_by_end_date_2 = Window.partitionBy("row_count", "status").orderBy("end_date_span_days_per_min_premium")

valid_policys = (
    df_policies
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

per_group = (
    valid_policys
    .groupBy(F.date_trunc("month", "start_date").alias("start_date_month"), "status")
    .agg(
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.count("*").alias("row_count"),
        F.datediff(F.max("end_date"), F.min("end_date")).alias("end_date_span_days"),
        F.min("premium").alias("min_premium"),
    )
    .filter(F.col("row_count") >= 2)
)

labeled = (
    per_group
    .select(
        "*",
        F.abs(F.col("min_premium") - 2120.0).alias("min_premium_distance"),
        F.round(F.col("end_date_span_days") / F.nullif(F.col("min_premium"), F.lit(0)), 3).alias("end_date_span_days_per_min_premium"),
        F.least(F.col("end_date_span_days"), F.col("median_premium")).alias("least_end_date_span_days_median_premium"),
    )
)

with_trends = (
    labeled
    .select(
        "*",
        F.lag("min_premium").over(w_status_by_end_date_span_days).alias("prev_min_premium"),
        F.sum("min_premium").over(w_status_row_count).alias("status_row_count_sum_min_premium"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.lag("row_count", 3).over(w_row_count_status_by_end_date).alias("prev3_row_count"),
        F.round(F.percent_rank().over(w_row_count_status_by_end_date), 3).alias("end_date_span_days_percent_rank"),
        F.ntile(5).over(w_row_count_status_by_end_date).alias("end_date_span_days_quintile"),
    )
)

metrics = (
    windowed
    .select(
        "*",
        F.lag("min_premium_distance", 1, 0).over(w_all_by_prev_min_premium).alias("prev_min_premium_distance"),
        F.size(F.collect_set("status").over(w_end_date_span_days_quintile)).alias("distinct_status_in_end_date_span_days_quintile_s"),
        F.dense_rank().over(w_row_count_status_by_end_date_2).alias("end_date_span_days_per_min_premium_dense_rank"),
        F.round(F.percent_rank().over(w_row_count_status_by_end_date_2), 3).alias("end_date_span_days_per_min_premium_percent_rank"),
        F.max("end_date_span_days_quintile").over(w_end_date_span_days_quintile).alias("end_date_span_days_quintile_start_date_month_mon"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.avg("row_count").alias("overall_avg_row_count")))
    .filter(F.col("row_count") > F.col("overall_avg_row_count"))
    .drop("overall_avg_row_count")
)

result = (
    with_activity
    .select(
        "start_date_month",
        "status",
        "median_premium",
        "row_count",
        "end_date_span_days",
        "min_premium",
        "min_premium_distance",
        "end_date_span_days_per_min_premium",
        "prev_min_premium",
        "status_row_count_sum_min_premium",
        "prev3_row_count",
        "prev_min_premium_distance",
        "distinct_status_in_end_date_span_days_quintile_s",
        "end_date_span_days_per_min_premium_dense_rank",
        "end_date_span_days_per_min_premium_percent_rank",
        "end_date_span_days_quintile_start_date_month_mon",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)
