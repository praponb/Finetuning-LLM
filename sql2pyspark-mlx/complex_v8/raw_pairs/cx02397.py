from pyspark.sql import functions as F

cleaned = (
    df_claims
    .filter(F.col("claimed_amount") <= 28900.9)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"),
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.month(F.col("c.reported_at")).isin(1, 3)), "left_anti")
)

with_details_policy = (
    qualified.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("a.team").alias("adjuster_team"),
    )
)

with_details_adjuster = (
    with_details_policy.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.policy_holder_id",
        "s.policy_product",
        "s.policy_end_date",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team_2"),
        F.col("p.product").alias("policy_product_2"),
    )
)

result = (
    with_details_adjuster
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "policy_holder_id",
        "policy_product",
        "policy_end_date",
        "adjuster_level",
        "adjuster_name",
        "policy_product_2",
        F.coalesce(F.col("adjuster_level"), F.lit(0)).alias("adjuster_level_filled"),
    )
    .orderBy(F.col("policy_id").desc(), "id")
    .limit(20)
)
