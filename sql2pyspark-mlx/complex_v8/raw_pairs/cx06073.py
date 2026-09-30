from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_joined_on_month_end_d = Window.partitionBy(F.month("holder_joined_on"), F.month("end_date")).orderBy("holder_joined_on", "id")
w_all_by_policy_count = Window.orderBy(F.col("policy_count").desc(), "id")
w_product_holder_joined_on_yea = Window.partitionBy("product", F.year("holder_joined_on"))
w_holder_segment_start_date_mo = Window.partitionBy("holder_segment", F.date_trunc("month", "start_date")).orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_policys = (
    df_policies
    .filter(F.col("product") == "LIFE")
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

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.round(F.avg("premium"), 2).alias("avg_policy_premium"))
)

with_counts = (
    filtered_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.avg_policy_premium")
)

status_stats = (
    with_counts
    .groupBy("status")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

vs_group = (
    with_counts.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

joined_holder = (
    vs_group.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.end_date",
        "s.median_premium",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

summarized_rows = (
    joined_holder.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

with_windows = (
    summarized_rows
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "median_premium",
        "holder_segment",
        "holder_risk_score",
        "holder_joined_on",
        "policy_count",
        F.row_number().over(w_holder_joined_on_month_end_d).alias("holder_joined_on_seq"),
        F.ntile(3).over(w_all_by_policy_count).alias("policy_count_tercile"),
        F.size(F.collect_set("holder_id").over(w_product_holder_joined_on_yea)).alias("distinct_holder_id_in_product_holder_joined_on_y"),
        F.first("holder_risk_score").over(w_holder_segment_start_date_mo).alias("first_holder_risk_score"),
        F.ntile(4).over(w_holder_joined_on_month_end_d).alias("holder_joined_on_quartile"),
        F.round(F.col("holder_risk_score") / F.nullif(F.sum("holder_risk_score").over(w_product_holder_joined_on_yea), F.lit(0)), 4).alias("holder_risk_score_share_of_product_holder_joined"),
    )
)

result = (
    with_windows
    .groupBy("start_date", "policy_count_tercile")
    .pivot("holder_segment", ["RETAIL", "VIP", "SME"])
    .agg(F.count("holder_joined_on_seq"))
)
