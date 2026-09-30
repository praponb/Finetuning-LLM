from pyspark.sql import functions as F

base = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.last_day(F.col("claim_date")).alias("claim_date_month_end"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_date_month_end",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
    )
)

with_details_policy = (
    with_parent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_date_month_end",
        "s.policy_holder_id",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

adjuster_id_stats = (
    with_details_policy
    .groupBy("adjuster_id")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

outliers = (
    with_details_policy.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "avg_approved_amount",
        "approved_amount_minus_avg",
    )
    .unpivot(
        ids=["id"],
        values=["claimed_amount", "approved_amount", "avg_approved_amount", "approved_amount_minus_avg"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
