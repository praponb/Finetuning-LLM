from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_region_by_avg_policy = Window.partitionBy("segment", "region").orderBy(F.col("avg_policy_premium").asc_nulls_last())
w_region_segment = Window.partitionBy("region", "segment")
w_joined_on_year_by_risk_score = Window.partitionBy(F.year("joined_on")).orderBy("risk_score_adj_vs_region_segment_avg")
w_all_by_joined_on = Window.orderBy(F.col("joined_on").desc_nulls_first(), "id")
w_joined_on_month = Window.partitionBy(F.month("joined_on"))

cleaned = (
    df_policyholders
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
        F.when(F.col("risk_score") >= 0.6, "large").when(F.col("risk_score") >= 0.3, "medium").otherwise("small").alias("risk_score_tier"),
    )
)

derived = (
    cleaned
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        "segment_ref",
        F.round(F.col("risk_score") * 1.1, 2).alias("risk_score_adj"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.round(F.avg("premium"), 2).alias("avg_policy_premium"))
)

with_stats = (
    derived.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.avg_policy_premium")
)

with_trends = (
    with_stats
    .select(
        "*",
        F.round(F.percent_rank().over(w_segment_region_by_avg_policy), 3).alias("avg_policy_premium_percent_rank"),
        F.round(F.col("risk_score_adj") - F.avg("risk_score_adj").over(w_region_segment), 2).alias("risk_score_adj_vs_region_segment_avg"),
    )
)

tagged = (
    with_trends
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "risk_score_adj",
        "avg_policy_premium_percent_rank",
        "risk_score_adj_vs_region_segment_avg",
        F.round(F.col("avg_policy_premium") / F.nullif(F.col("risk_score_adj"), F.lit(0)), 3).alias("avg_policy_premium_per_risk_score_adj"),
        F.date_add(F.col("joined_on"), 365).alias("joined_on_plus_365d"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "holder_name",
        "risk_score",
        "risk_score_adj",
        "avg_policy_premium_percent_rank",
        "risk_score_adj_vs_region_segment_avg",
        "avg_policy_premium_per_risk_score_adj",
        "joined_on_plus_365d",
        F.rank().over(w_joined_on_year_by_risk_score).alias("risk_score_adj_vs_region_segment_avg_rank"),
        F.row_number().over(w_all_by_joined_on).alias("seq_no"),
        F.lag("joined_on_plus_365d", 2).over(w_all_by_joined_on).alias("prev2_joined_on_plus_365d"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_joined_on_month), F.lit(0)), 4).alias("risk_score_share_of_joined_on_month"),
        F.lead("joined_on_plus_365d", 2).over(w_all_by_joined_on).alias("next2_joined_on_plus_365d"),
        F.round(F.avg("risk_score").over(w_joined_on_month), 2).alias("joined_on_month_avg_risk_score"),
    )
)

filtered = (
    ranked
    .crossJoin(ranked.agg(F.avg("avg_policy_premium_percent_rank").alias("overall_avg_avg_policy_premium_percent_rank")))
    .filter(F.col("avg_policy_premium_percent_rank") > F.col("overall_avg_avg_policy_premium_percent_rank"))
    .drop("overall_avg_avg_policy_premium_percent_rank")
)

result = (
    filtered
    .select(
        "id",
        "holder_name",
        "risk_score_adj",
        "avg_policy_premium_percent_rank",
        "risk_score_adj_vs_region_segment_avg",
        "risk_score_share_of_joined_on_month",
        "next2_joined_on_plus_365d",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("joined_on_plus_365d")).alias("days_since_joined_on_plus_365d"),
        F.date_format(F.col("joined_on_plus_365d"), "yyyy-MM-dd").alias("joined_on_plus_365d_label"),
    )
    .orderBy(
        "risk_score_share_of_joined_on_month",
        F.col("joined_on_plus_365d_label").asc(),
    )
)
