from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("claim_type").isin("fire", "flood", "medical")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.round(F.col("approved_amount") * 1.5, 2).alias("approved_amount_adj"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
        (F.col("reported_at") + F.expr("INTERVAL 7 DAYS")).alias("reported_at_plus_7d"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("reported_at").alias("last_claim_reported_at"),
    )
)

with_counts = (
    scoped_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_reported_at",
    )
)

combined_details_policy = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.premium").between(986.9, 3544.1))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount_adj",
        "s.claim_type_ref",
        "s.claim_count",
        "s.last_claim_reported_at",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
    )
)

with_details_adjuster = (
    combined_details_policy.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.team") != "Travel")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount_adj",
        "s.claim_type_ref",
        "s.claim_count",
        "s.policy_start_date",
        "s.policy_holder_id",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

combined_details_holder = (
    with_details_adjuster.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.policy_holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

result = (
    combined_details_holder
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("F%"))
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type_ref",
        "policy_start_date",
        "holder_region",
        "holder_joined_on",
        F.abs(F.col("approved_amount_adj") - 26250.0).alias("approved_amount_adj_distance"),
    )
    .orderBy(F.col("policy_start_date").desc_nulls_first(), "id")
    .limit(10)
)
