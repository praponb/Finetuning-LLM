from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_median_premium_prior = Window.orderBy(F.col("median_premium").desc(), "end_date_ym").rowsBetween(-6, -1)
w_all_by_median_premium = Window.orderBy(F.col("median_premium").desc(), "end_date_ym")
w_all_by_stddev_holder_risk_sc = Window.orderBy("stddev_holder_risk_score", "end_date_ym").rowsBetween(-6, Window.currentRow)
w_all_by_stddev_holder_risk_sc_2 = Window.orderBy(F.col("stddev_holder_risk_score").desc(), "end_date_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_median_premium_seq_by_runnin = Window.partitionBy("median_premium_seq").orderBy("running_count", "end_date_ym").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_median_premium_seq_by_runnin_2 = Window.partitionBy("median_premium_seq").orderBy("running_count", "end_date_ym")

cleaned = (
    df_policies
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

with_parent_holder = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.joined_on").alias("holder_joined_on"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment"),
    )
)

totals = (
    with_parent_holder
    .filter(F.col("premium") < 4678.8)
    .groupBy(F.date_format("end_date", "yyyy-MM").alias("end_date_ym"))
    .agg(
        F.percentile_approx("premium", 0.5).alias("median_premium"),
        F.round(F.stddev("holder_risk_score"), 2).alias("stddev_holder_risk_score"),
        F.max("premium").alias("max_premium_check"),
    )
    .filter(F.col("max_premium_check") > 2033.8)
    .drop("max_premium_check")
)

metrics = (
    totals
    .select(
        "end_date_ym",
        "median_premium",
        "stddev_holder_risk_score",
        F.sum("median_premium").over(w_all_by_median_premium_prior).alias("median_premium_sum_6_prior"),
        (F.col("stddev_holder_risk_score") - F.lag("stddev_holder_risk_score").over(w_all_by_median_premium)).alias("stddev_holder_risk_score_change"),
        F.sum("median_premium").over(w_all_by_stddev_holder_risk_sc).alias("median_premium_sum_7"),
        F.count(F.lit(1)).over(w_all_by_stddev_holder_risk_sc_2).alias("running_count"),
        F.row_number().over(w_all_by_median_premium).alias("median_premium_seq"),
    )
)

screened = (
    metrics
    .crossJoin(metrics.agg(F.avg("stddev_holder_risk_score_change").alias("overall_avg_stddev_holder_risk_score_change")))
    .filter(F.col("stddev_holder_risk_score_change") > F.col("overall_avg_stddev_holder_risk_score_change"))
    .drop("overall_avg_stddev_holder_risk_score_change")
)

with_trends = (
    screened
    .select(
        "end_date_ym",
        "median_premium_sum_6_prior",
        "stddev_holder_risk_score_change",
        "median_premium_sum_7",
        "median_premium_seq",
        F.last("running_count").over(w_median_premium_seq_by_runnin).alias("last_running_count"),
        (F.col("running_count") - F.lag("running_count").over(w_median_premium_seq_by_runnin_2)).alias("running_count_change"),
    )
)

without_matches = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("stddev_holder_risk_score_change").alias("overall_avg_stddev_holder_risk_score_change")))
    .filter(F.col("stddev_holder_risk_score_change") > F.col("overall_avg_stddev_holder_risk_score_change"))
    .drop("overall_avg_stddev_holder_risk_score_change")
)

result = (
    without_matches
    .select(
        "end_date_ym",
        "median_premium_sum_6_prior",
        "median_premium_seq",
        "last_running_count",
        F.substring(F.trim(F.col("end_date_ym")), 1, 3).alias("end_date_ym_prefix"),
    )
    .orderBy(F.col("median_premium_sum_6_prior").asc(), "end_date_ym")
    .limit(10)
)
