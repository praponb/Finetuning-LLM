from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("approved_amount") > 2569.2) | (F.col("adjuster_id") <= 7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
    )
)

enriched = (
    filtered_claims
    .filter(F.col("status") == "PAID")
    .select(
        "*",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.count(F.when(~F.col("status").isin("REJECTED", "APPROVED"), 1)).alias("claim_status_count"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

screened = (
    with_counts.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_type") == "medical") & (F.col("c.status").isNotNull()), "left_anti")
    .filter(F.col("claimed_amount") <= 43500.4)
)

with_details = (
    screened.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") == "Travel"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_date",
        "s.status_ref",
        "s.approved_amount_plus_claimed_amount",
        "s.last_claim_date",
        "s.claim_status_count",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    with_details
    .filter(F.col("approved_amount_plus_claimed_amount") <= 70000.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_date",
        "status_ref",
        "approved_amount_plus_claimed_amount",
        "adjuster_name",
        "adjuster_level",
        F.round(F.col("approved_amount") * 1.1, 2).alias("approved_amount_adj"),
    )
    .orderBy(F.col("claim_type").desc())
)
