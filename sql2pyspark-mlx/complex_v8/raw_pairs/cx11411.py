from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_id_claim_claim_type_by = Window.partitionBy("claim_id", "claim_claim_type").orderBy("claimed_amount", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_id_claim_claim_type_by_2 = Window.partitionBy("claim_id", "claim_claim_type").orderBy("claimed_amount", "id", "claim_id")
w_claim_type_claim_claim_type = Window.partitionBy("claim_type", "claim_claim_type")
w_claim_id_by_avg_claimed_amou = Window.partitionBy("claim_id").orderBy(F.col("avg_claimed_amount_minus_approved_amount").desc(), "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_claims
    .filter(~F.col("claim_type").isin("flood", "fire"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.year(F.col("reported_at")).alias("reported_at_year"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        (F.col("claimed_amount") - F.col("approved_amount")).alias("claimed_amount_minus_approved_amount"),
    )
)

adjuster_id_stats = (
    scoped
    .groupBy("adjuster_id")
    .agg(F.avg("claimed_amount_minus_approved_amount").alias("avg_claimed_amount_minus_approved_amount"))
)

above_average = (
    scoped.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount_minus_approved_amount") > F.col("g.avg_claimed_amount_minus_approved_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount_minus_approved_amount",
        F.round(F.col("s.claimed_amount_minus_approved_amount") - F.col("g.avg_claimed_amount_minus_approved_amount"), 2).alias("claimed_amount_minus_approved_amount_minus_avg"),
    )
)

without_matches = (
    above_average.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isNotNull()) & (~F.col("c.claim_type").isin("theft", "collision")), "left_anti")
)

scored = (
    without_matches
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "reported_at_year",
        "claimed_amount_minus_approved_amount",
        "avg_claimed_amount_minus_approved_amount",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
        F.round(F.col("claimed_amount") * 1.1, 2).alias("claimed_amount_adj"),
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
    )
)

with_details = (
    scored.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at_year",
        "s.claimed_amount_minus_approved_amount",
        "s.avg_claimed_amount_minus_approved_amount",
        "s.reported_at_label",
        "s.claimed_amount_adj",
        "s.claim_type_prefix",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("po.segment").alias("holder_segment"),
        "po.holder_name",
    )
)

detail_rows_claims = (
    with_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.avg_claimed_amount_minus_approved_amount",
        "s.reported_at_label",
        "s.claimed_amount_adj",
        "s.claim_type_prefix",
        "s.policy_holder_id",
        "s.policy_premium",
        "s.holder_segment",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

with_windows = (
    detail_rows_claims
    .select(
        "*",
        F.nth_value("claimed_amount", 2).over(w_claim_id_claim_claim_type_by).alias("second_claimed_amount"),
        (F.col("claimed_amount_adj") - F.lag("claimed_amount_adj").over(w_claim_id_claim_claim_type_by_2)).alias("claimed_amount_adj_change"),
        F.min("claim_claimed_amount").over(w_claim_type_claim_claim_type).alias("claim_type_claim_claim_type_min_claim_claimed_am"),
        F.sum("claimed_amount").over(w_claim_type_claim_claim_type).alias("claim_type_claim_claim_type_sum_claimed_amount"),
        F.round(F.avg("claim_claimed_amount").over(w_claim_type_claim_claim_type), 2).alias("claim_type_claim_claim_type_avg_claim_claimed_am"),
        F.sum("claimed_amount").over(w_claim_id_by_avg_claimed_amou).alias("running_claimed_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "claim_type",
        "claimed_amount",
        "claimed_amount_adj",
        "claim_type_prefix",
        "policy_premium",
        "claim_approved_amount",
        "claim_claimed_amount",
        "second_claimed_amount",
        "claimed_amount_adj_change",
        "claim_type_claim_claim_type_min_claim_claimed_am",
        "claim_type_claim_claim_type_avg_claim_claimed_am",
        F.round(F.col("second_claimed_amount") / F.nullif(F.col("claim_claimed_amount"), F.lit(0)), 3).alias("second_claimed_amount_per_claim_claimed_amount"),
    )
    .orderBy(
        F.col("claimed_amount").desc(),
        F.col("claimed_amount_adj_change").desc(),
    )
)
