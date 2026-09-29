from pyspark.sql import functions as F

filtered = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.when(F.col("status").isin("IN_REVIEW", "REJECTED"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        F.when(F.col("approved_amount") >= 18478.3, "HIGH").when(F.col("approved_amount") >= 1364.5, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)

enriched_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.team").isin("Health", "Motor", "Property"))
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

annotated = (
    enriched_adjuster
    .select(
        "*",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

enriched = (
    annotated.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("a.team").alias("adjuster_team_2"),
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "reported_at",
        "status_group",
        "claim_type_slug",
        "approved_amount_tier",
        "adjuster_level",
        "adjuster_team",
        "adjuster_name",
        "claimed_amount_bucket",
        "adjuster_team_2",
        "policy_status",
        "policy_product",
        F.concat_ws("-", F.col("approved_amount_tier"), F.col("adjuster_team")).alias("approved_amount_tier_adjuster_team_label"),
    )
)
