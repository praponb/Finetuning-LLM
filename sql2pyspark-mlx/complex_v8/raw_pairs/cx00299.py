from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("status") == "IN_REVIEW")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("claimed_amount") >= 30115.9, "A").when(F.col("claimed_amount") >= 10518.4, "B").otherwise("C").alias("claimed_amount_tier"),
        F.date_trunc("quarter", F.col("reported_at")).alias("reported_at_quarter"),
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

combined_details_policy = (
    recent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_tier",
        "s.reported_at_quarter",
        "s.claim_type_clean",
        F.col("p.holder_id").alias("policy_holder_id"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

enriched = (
    combined_details_policy.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.length(F.trim(F.col("a.adjuster_name"))) >= 8), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.reported_at",
        "s.reported_at_quarter",
        "s.claim_type_clean",
        "s.policy_holder_id",
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team_2"),
    )
)

result = (
    enriched
    .groupBy("policy_id")
    .pivot("status", ["REJECTED", "APPROVED"])
    .agg(F.sum("adjuster_level"))
    .orderBy("policy_id")
)
