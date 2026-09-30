from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("claimed_amount") >= 5774.6)
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
    )
)

scored = (
    cleaned
    .filter(F.col("claimed_amount") > 3703.0)
    .select(
        "*",
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("status").isin("IN_REVIEW", "REJECTED"), 1)).alias("claim_status_count"),
    )
)

summarized_rows = (
    scored.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

enriched = (
    summarized_rows.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team") != "Property"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    enriched
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "least_claimed_amount_approved_amount",
        "reported_at_date",
        "claim_status_count",
        "policy_product",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
    .orderBy(F.col("status").desc())
)
