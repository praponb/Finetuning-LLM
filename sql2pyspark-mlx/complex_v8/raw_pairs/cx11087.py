from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("approved_amount").between(0.0, 39988.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") <= 32878.5) & (F.col("c.claim_type").isin("flood", "theft", "collision")), "left_anti")
    .filter(F.col("claim_date").isNotNull())
)

with_children_claims = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "flood", 1)).alias("claim_claim_type_count"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    with_children_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

without_matches_2 = (
    summarized_rows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") < 23120.7), "left_anti")
    .filter(~F.col("claim_type").isin("flood", "medical"))
)

tagged = (
    without_matches_2
    .filter(F.col("claim_approved_amount") < 22250.6)
    .select(
        "*",
        F.round(F.col("approved_amount") * 1.1, 2).alias("approved_amount_adj"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

result = (
    tagged
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "claim_approved_amount",
        "claim_reported_at",
        "claim_claimed_amount",
        "claim_status",
        "claim_type_or_unknown",
        "claim_type_slug",
        F.date_trunc("week", F.col("claim_reported_at")).alias("claim_reported_at_week"),
        F.length(F.trim(F.col("claim_type_slug"))).alias("claim_type_slug_len"),
    )
    .orderBy("claim_claimed_amount")
)
