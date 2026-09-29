from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_start_date_month_b = Window.partitionBy("holder_id", F.month("start_date")).orderBy(F.col("days_start_date_to_end_date").desc(), "id")
w_holder_id_start_date_month_b_2 = Window.partitionBy("holder_id", F.month("start_date")).orderBy(F.col("days_start_date_to_end_date").desc())
w_start_date_month = Window.partitionBy(F.date_trunc("month", "start_date"))
w_holder_id_by_start_date_year = Window.partitionBy("holder_id").orderBy(F.col("start_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_product_end_date_year_by_pre = Window.partitionBy("product", F.year("end_date")).orderBy(F.col("premium").desc(), "id")
w_product_end_date_year_by_pre_2 = Window.partitionBy("product", F.year("end_date")).orderBy(F.col("premium").desc(), "id").rowsBetween(-3, -1)
w_product_end_date_month = Window.partitionBy("product", F.month("end_date"))
w_all_by_premium_quintile = Window.orderBy("premium_quintile", "id")
w_distinct_status_in_product_e = Window.partitionBy("distinct_status_in_product_end_date_month")
w_status_by_policy_product_cou = Window.partitionBy("status").orderBy(F.col("policy_product_count_avg_3_prior").desc(), "id")

filtered = (
    df_policies
    .filter((~F.col("product").isin("HEALTH", "AUTO")) & (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

ranked = (
    filtered
    .select(
        "*",
        F.row_number().over(w_holder_id_start_date_month_b).alias("rn"),
        F.round(F.percent_rank().over(w_holder_id_start_date_month_b_2), 3).alias("days_start_date_to_end_date_percent_rank"),
    )
)

with_windows = (
    ranked
    .withColumn("rows_in_start_date_month", F.count(F.lit(1)).over(w_start_date_month))
    .withColumn("second_days_start_date_to_end_date", F.nth_value("days_start_date_to_end_date", 2).over(w_holder_id_by_start_date_year))
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(~F.col("product").isin("AUTO", "HEALTH"), 1)).alias("policy_product_count"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

metrics = (
    summarized_rows
    .select(
        "id",
        "product",
        "premium",
        "status",
        "start_date_year",
        "days_start_date_to_end_date",
        "rn",
        "rows_in_start_date_month",
        "second_days_start_date_to_end_date",
        F.ntile(5).over(w_product_end_date_year_by_pre).alias("premium_quintile"),
        F.round(F.avg("policy_product_count").over(w_product_end_date_year_by_pre_2), 2).alias("policy_product_count_avg_3_prior"),
        F.size(F.collect_set("status").over(w_product_end_date_month)).alias("distinct_status_in_product_end_date_month"),
    )
)

status_stats = (
    metrics
    .groupBy("status")
    .agg(F.percentile_approx("days_start_date_to_end_date", 0.5).alias("median_days_start_date_to_end_date"))
)

above_average = (
    metrics.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.days_start_date_to_end_date") >= F.col("g.median_days_start_date_to_end_date"))
    .select(
        "s.*",
        "g.median_days_start_date_to_end_date",
        F.round(F.col("s.days_start_date_to_end_date") - F.col("g.median_days_start_date_to_end_date"), 2).alias("days_start_date_to_end_date_minus_median"),
    )
)

ranked_2 = (
    above_average
    .select(
        "id",
        "product",
        "premium",
        "status",
        "days_start_date_to_end_date",
        "rn",
        "rows_in_start_date_month",
        "second_days_start_date_to_end_date",
        "distinct_status_in_product_end_date_month",
        "median_days_start_date_to_end_date",
        "days_start_date_to_end_date_minus_median",
        F.round(F.percent_rank().over(w_all_by_premium_quintile), 3).alias("premium_quintile_percent_rank"),
        F.round(F.col("premium_quintile") - F.avg("premium_quintile").over(w_distinct_status_in_product_e), 2).alias("premium_quintile_vs_distinct_status_in_product_e"),
        F.rank().over(w_all_by_premium_quintile).alias("premium_quintile_rank"),
        F.round(F.percent_rank().over(w_status_by_policy_product_cou), 3).alias("policy_product_count_avg_3_prior_percent_rank"),
        F.count(F.lit(1)).over(w_distinct_status_in_product_e).alias("rows_in_distinct_status_in_product_end_date_mont"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "product",
        "days_start_date_to_end_date",
        "rn",
        "distinct_status_in_product_end_date_month",
        "median_days_start_date_to_end_date",
        "days_start_date_to_end_date_minus_median",
        "premium_quintile_percent_rank",
        "premium_quintile_vs_distinct_status_in_product_e",
        "premium_quintile_rank",
        "policy_product_count_avg_3_prior_percent_rank",
        "rows_in_distinct_status_in_product_end_date_mont",
    )
    .orderBy(F.col("rn").desc())
)
