from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("claimed_amount") > 7204.1) & (F.col("reported_at") >= F.lit("2025-01-31 11:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

qualified = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount") < 24718.5) & (F.col("c.policy_id").isNotNull()), "left_anti")
)

with_children = (
    qualified.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.status_slug",
        "s.greatest_approved_amount_claimed_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        "c.claim_date",
    )
)

enriched = (
    with_children
    .filter(F.col("status_slug").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "status",
        "greatest_approved_amount_claimed_amount",
        "claim_id",
        "claim_status",
        "claim_date",
        F.when(F.col("status").isin("APPROVED", "OPEN"), "group_a").when(F.col("status") == "IN_REVIEW", "group_b").otherwise("other").alias("status_group"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.concat_ws("|", F.col("claim_status"), F.col("status")).alias("claim_status_status_label"),
    )
)

exploded_detail_claims = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        "s.greatest_approved_amount_claimed_amount",
        "s.claim_id",
        "s.claim_status",
        "s.claim_date",
        "s.claim_status_status_label",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

result = (
    exploded_detail_claims
    .filter(F.col("claim_status_status_label").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "status",
        "greatest_approved_amount_claimed_amount",
        "claim_id",
        "claim_date",
        "claim_id_2",
        "claim_approved_amount",
        "claim_claimed_amount",
        "claim_reported_at",
        "claim_policy_id",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
    .orderBy(F.col("id").asc())
)
