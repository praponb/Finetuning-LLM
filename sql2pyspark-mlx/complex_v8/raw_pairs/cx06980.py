from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

per_group = (
    covered
    .groupBy(
        F.date_trunc("month", "claim_date").alias("claim_date_month"),
        "policy_id",
        "claim_type",
    )
    .agg(
        F.sum(F.when(F.col("claim_type") != "collision", 1).otherwise(0)).alias("n_claim_type_collision"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.avg("claimed_amount").alias("avg_claimed_amount_check"),
    )
    .filter(F.col("avg_claimed_amount_check") > 17585.2)
    .drop("avg_claimed_amount_check")
)

with_children = (
    per_group.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()), "left")
    .select(
        "s.claim_date_month",
        "s.policy_id",
        "s.claim_type",
        "s.n_claim_type_collision",
        "s.distinct_policy_ids",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

with_parent_adjuster = (
    with_children.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.claim_adjuster_id"), "inner")
    .select(
        "s.claim_date_month",
        "s.policy_id",
        "s.claim_type",
        "s.n_claim_type_collision",
        "s.distinct_policy_ids",
        "s.claim_id",
        "s.claim_adjuster_id",
        "s.claim_approved_amount",
        "s.claim_reported_at",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

result = (
    with_parent_adjuster
    .select(
        "claim_date_month",
        "policy_id",
        "claim_type",
        "n_claim_type_collision",
        "distinct_policy_ids",
        "claim_id",
        "claim_approved_amount",
        "claim_reported_at",
        "adjuster_team",
        "adjuster_level",
        "adjuster_name",
    )
    .orderBy(
        F.col("claim_type").desc(),
        F.col("claim_date_month").desc(),
        "policy_id",
        "claim_id",
    )
    .limit(10)
)
