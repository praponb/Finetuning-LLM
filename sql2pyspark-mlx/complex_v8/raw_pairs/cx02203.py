from pyspark.sql import functions as F

base = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
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
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

without_matches = (
    base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") < F.lit("2025-02-14").cast("date")) & (F.col("p.product").isin("LIFE", "AUTO", "HEALTH")), "left_semi")
)

with_details_adjuster = (
    without_matches.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(~F.col("a.team").isin("Motor", "Travel"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        F.col("a.team").alias("adjuster_team"),
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
    )
)

screened = (
    with_details_adjuster
    .crossJoin(with_details_adjuster.agg(F.max("approved_amount").alias("overall_max_approved_amount")))
    .filter(F.col("approved_amount") < F.col("overall_max_approved_amount"))
    .drop("overall_max_approved_amount")
)

result = (
    screened
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "adjuster_team",
        "policy_premium",
        F.when(F.col("adjuster_team").isin("Motor", "Property"), "group_a").when(F.col("adjuster_team") == "Travel", "group_b").otherwise("other").alias("adjuster_team_group"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
    .orderBy(F.col("claim_date").desc_nulls_first(), F.col("status").asc())
)
