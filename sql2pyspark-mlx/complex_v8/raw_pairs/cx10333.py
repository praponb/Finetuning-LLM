from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_all_by_approved_amount_full = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount_ties = Window.orderBy("approved_amount")
w_adjuster_id_approved_amount = Window.partitionBy("adjuster_id", "approved_amount_rank")

recent = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(10, 13))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

screened = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") >= 1), "left_anti")
)

screened_2 = (
    screened.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") < 43500.4), "left_anti")
)

ranked = (
    screened_2
    .select(
        "*",
        F.count(F.lit(1)).over(w_policy_id_adjuster_id).alias("rows_in_policy_id_adjuster_id"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_adjuster_id"),
        F.rank().over(w_all_by_approved_amount).alias("approved_amount_rank"),
        F.nth_value("claimed_amount", 2).over(w_all_by_approved_amount_full).alias("second_claimed_amount"),
        F.lead("claimed_amount", 2).over(w_all_by_approved_amount).alias("next2_claimed_amount"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_cume_dist"),
        F.round(F.avg("approved_amount_rank").over(w_adjuster_id_approved_amount), 2).alias("adjuster_id_approved_amount_rank_avg_approved_am"),
        F.round(F.col("next2_claimed_amount") - F.avg("next2_claimed_amount").over(w_adjuster_id_approved_amount), 2).alias("next2_claimed_amount_vs_adjuster_id_approved_amo"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_approved_amount), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_approved_am"),
    )
)

policy_id_stats = (
    metrics
    .groupBy("policy_id")
    .agg(F.max("next2_claimed_amount").alias("max_next2_claimed_amount"))
)

compared = (
    metrics.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.next2_claimed_amount") == F.col("g.max_next2_claimed_amount"))
    .select(
        "s.*",
        "g.max_next2_claimed_amount",
        F.round(F.col("s.next2_claimed_amount") - F.col("g.max_next2_claimed_amount"), 2).alias("next2_claimed_amount_minus_max"),
    )
)

annotated = (
    compared
    .select(
        "*",
        (F.floor(F.col("second_claimed_amount") / 10000) * 10000).alias("second_claimed_amount_bucket"),
        F.when(F.col("claimed_amount") >= 29191.2, "HIGH").when(F.col("claimed_amount") >= 12010.5, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "rows_in_policy_id_adjuster_id",
        "approved_amount_share_of_policy_id_adjuster_id",
        "approved_amount_rank",
        "second_claimed_amount",
        "next2_claimed_amount",
        "approved_amount_cume_dist",
        "adjuster_id_approved_amount_rank_avg_approved_am",
        "next2_claimed_amount_vs_adjuster_id_approved_amo",
        "approved_amount_share_of_adjuster_id_approved_am",
        "next2_claimed_amount_minus_max",
        "second_claimed_amount_bucket",
        "claimed_amount_tier",
        "days_since_reported_at",
        F.when(F.col("days_since_reported_at") >= 560, "HIGH").when(F.col("days_since_reported_at") >= 140, "MEDIUM").otherwise("LOW").alias("days_since_reported_at_tier"),
        F.abs(F.col("next2_claimed_amount") - 20843.1).alias("next2_claimed_amount_distance"),
    )
    .orderBy(
        "adjuster_id_approved_amount_rank_avg_approved_am",
        "next2_claimed_amount_vs_adjuster_id_approved_amo",
    )
)
