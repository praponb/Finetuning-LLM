from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_all_by_reported_at = Window.orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_all_by_reported_at_ties = Window.orderBy(F.col("reported_at").asc_nulls_last())
w_all_by_total_minutes_since_p = Window.orderBy("total_minutes_since_prev_reported_at", "approved_amount_tier", "adjuster_id").rowsBetween(-6, -1)
w_all_by_total_minutes_since_p_2 = Window.orderBy("total_minutes_since_prev_reported_at")

base = (
    df_claims
    .filter(F.col("claimed_amount") <= 34763.4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
        F.when(F.col("approved_amount") >= 16404.6, "A").when(F.col("approved_amount") >= 1273.7, "B").otherwise("C").alias("approved_amount_tier"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_reported_at))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.lag("approved_amount").over(w_all_by_reported_at).alias("prev_approved_amount"),
        F.lead("approved_amount", 1, 0).over(w_all_by_reported_at).alias("next_approved_amount"),
        F.round(F.cume_dist().over(w_all_by_reported_at_ties), 3).alias("reported_at_cume_dist"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
    )
)

annotated = (
    windowed
    .filter(F.col("policy_id") <= 24)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        "approved_amount_tier",
        "approved_amount_share_of_policy_id",
        "minutes_since_prev_reported_at",
        "next_approved_amount",
        "reported_at_cume_dist",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.length(F.trim(F.col("approved_amount_tier"))).alias("approved_amount_tier_len"),
    )
)

stats = (
    annotated
    .filter(F.col("reported_at_cume_dist") > 0.1)
    .groupBy("approved_amount_tier", "adjuster_id")
    .agg(
        F.countDistinct("approved_amount_tier").alias("distinct_approved_amount_tiers"),
        F.max("minutes_since_prev_reported_at").alias("max_minutes_since_prev_reported_at"),
        F.round(F.sum("minutes_since_prev_reported_at"), 2).alias("total_minutes_since_prev_reported_at"),
        F.round(100 * F.count(F.when(F.col("status") == "IN_REVIEW", 1)) / F.count("*"), 1).alias("pct_status_in_review"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.min("approved_amount").alias("min_approved_amount"),
        F.max("approved_amount_share_of_policy_id").alias("max_approved_amount_share_of_policy_id_check"),
    )
    .filter(F.col("max_approved_amount_share_of_policy_id_check") > 0.2)
    .drop("max_approved_amount_share_of_policy_id_check")
)

enriched = (
    stats.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.approved_amount_tier",
        "s.adjuster_id",
        "s.distinct_approved_amount_tiers",
        "s.total_minutes_since_prev_reported_at",
        "s.min_approved_amount",
        F.col("a.team").alias("adjuster_team"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.avg("min_approved_amount").over(w_all_by_total_minutes_since_p), 2).alias("min_approved_amount_avg_6_prior"),
        F.round(F.cume_dist().over(w_all_by_total_minutes_since_p_2), 3).alias("total_minutes_since_prev_reported_at_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "approved_amount_tier",
        "adjuster_id",
        "distinct_approved_amount_tiers",
        "total_minutes_since_prev_reported_at",
        "min_approved_amount",
        "adjuster_team",
        "min_approved_amount_avg_6_prior",
        F.substring(F.trim(F.col("approved_amount_tier")), 1, 2).alias("approved_amount_tier_prefix"),
    )
    .orderBy(F.col("approved_amount_tier_prefix").desc(), "adjuster_team")
)
