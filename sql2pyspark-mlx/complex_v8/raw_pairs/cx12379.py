from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count_by_total_holder_ri = Window.partitionBy("row_count").orderBy(F.col("total_holder_risk_score").desc(), "end_date_month_end_ym", "holder_id")
w_distinct_holder_segment_2s = Window.partitionBy("distinct_holder_segment_2s")

scoped_policys = (
    df_policies
    .filter(F.col("premium") >= 2111.3)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.abs(F.col("premium") - 3532.7).alias("premium_distance"),
        F.date_format(F.col("end_date"), "yyyy-MM-dd").alias("end_date_label"),
        F.last_day(F.col("end_date")).alias("end_date_month_end"),
    )
)

derived = (
    scoped_policys
    .select(
        "*",
        (F.floor(F.col("premium_distance") / 1000) * 1000).alias("premium_distance_bucket"),
        F.date_add(F.col("end_date_month_end"), 90).alias("end_date_month_end_plus_90d"),
        F.coalesce(F.col("premium_distance"), F.lit(0.0)).alias("premium_distance_filled"),
    )
)

enriched = (
    derived.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

enriched_holder = (
    with_stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.premium_distance",
        "s.end_date_label",
        "s.end_date_month_end",
        "s.premium_distance_filled",
        "s.holder_segment",
        "s.holder_region",
        "s.policy_count",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.segment").alias("holder_segment_2"),
    )
)

per_group = (
    enriched_holder
    .filter(F.col("policy_count") < 6)
    .groupBy(
        F.date_format("end_date_month_end", "yyyy-MM").alias("end_date_month_end_ym"),
        "holder_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.sum("holder_risk_score"), 2).alias("total_holder_risk_score"),
        F.countDistinct("holder_segment_2").alias("distinct_holder_segment_2s"),
    )
    .filter(F.col("row_count") >= 2)
)

windowed = (
    per_group
    .select(
        "*",
        F.lead("row_count", 2, 0).over(w_row_count_by_total_holder_ri).alias("next2_row_count"),
        F.dense_rank().over(w_row_count_by_total_holder_ri).alias("total_holder_risk_score_dense_rank"),
        F.round(F.col("distinct_holder_segment_2s") / F.nullif(F.sum("distinct_holder_segment_2s").over(w_distinct_holder_segment_2s), F.lit(0)), 4).alias("distinct_holder_segment_2s_share_of_distinct_hol"),
    )
)

result = (
    windowed
    .select(
        "end_date_month_end_ym",
        "holder_id",
        "total_holder_risk_score",
        "distinct_holder_segment_2s",
        "next2_row_count",
        "total_holder_risk_score_dense_rank",
        F.length(F.trim(F.col("end_date_month_end_ym"))).alias("end_date_month_end_ym_len"),
    )
    .orderBy(F.col("total_holder_risk_score").desc_nulls_first(), "next2_row_count")
)
