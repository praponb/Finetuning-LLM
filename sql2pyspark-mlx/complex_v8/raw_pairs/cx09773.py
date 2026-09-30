from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-04-25").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.month(F.col("claim_date")).alias("claim_date_month"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

status_stats = (
    recent_claims
    .groupBy("status")
    .agg(F.avg("claim_date_month").alias("avg_claim_date_month"))
)

outliers = (
    recent_claims.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.claim_date_month") > F.col("g.avg_claim_date_month"))
    .select(
        "s.*",
        "g.avg_claim_date_month",
        F.round(F.col("s.claim_date_month") - F.col("g.avg_claim_date_month"), 2).alias("claim_date_month_minus_avg"),
    )
)

scored = (
    outliers
    .filter(F.col("reported_at") < F.lit("2025-03-02 12:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "claimed_amount_per_approved_amount",
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.round(F.col("avg_claim_date_month") * 1.5, 2).alias("avg_claim_date_month_adj"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

summarized_rows = (
    scored.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.claimed_amount_per_approved_amount",
        "s.avg_claim_date_month_adj",
        "s.first_claim_reported_at",
        F.col("p.status").alias("policy_status"),
        F.col("a.team").alias("adjuster_team"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.min("claimed_amount").alias("min_claim_claimed_amount_2"),
    )
)

with_counts = (
    combined_details.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.min_claim_claimed_amount", "c.min_claim_claimed_amount_2")
)

combined_details_policy = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select("s.*", F.col("p.holder_id").alias("policy_holder_id"), "a.adjuster_name")
)

result = (
    combined_details_policy
    .filter(F.col("claim_type").isin("flood", "delay", "medical"))
    .select("avg_claim_date_month_adj", "claim_type", "min_claim_claimed_amount")
    .distinct()
)
