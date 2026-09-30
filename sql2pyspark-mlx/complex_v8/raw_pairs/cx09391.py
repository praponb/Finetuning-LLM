from pyspark.sql import functions as F

covered = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_30d"),
    )
)

with_parent_policy = (
    covered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.end_date") < F.lit("2026-09-23").cast("date"))
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_30d",
        "s.prior_approved_amount_30d",
        F.col("p.end_date").alias("policy_end_date"),
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

with_stats = (
    with_parent_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_reported_at",
    )
)

line_level_claims = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") != "OPEN"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.prior_rows_30d",
        "s.policy_end_date",
        "s.claim_count",
        "s.last_claim_reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

result = (
    line_level_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "prior_rows_30d",
        "policy_end_date",
        "claim_count",
        "last_claim_reported_at",
        "claim_id",
        "claim_claimed_amount",
        "claim_status",
        "claim_adjuster_id",
    )
)
