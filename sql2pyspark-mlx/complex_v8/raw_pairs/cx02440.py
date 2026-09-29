from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter(F.col("approved_amount") > 2272.7)
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
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
        F.date_trunc("month", F.col("claim_date")).alias("claim_date_month"),
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.year(F.col("p.start_date")) == 2025), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.status",
        "s.least_claimed_amount_approved_amount",
        "s.claim_date_month",
        F.col("p.premium").alias("policy_premium"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

with_parent_policy = (
    combined_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium_2"),
    )
)

summary = (
    with_parent_policy
    .groupBy(
        "adjuster_id",
        F.date_trunc("month", "claim_date_month").alias("claim_date_month_month"),
    )
    .agg(
        F.round(F.sum("policy_premium_2") / F.nullif(F.sum("policy_premium"), F.lit(0)), 3).alias("policy_premium_2_to_policy_premium_ratio"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("least_claimed_amount_approved_amount").alias("max_least_claimed_amount_approved_amount_check"),
    )
    .filter(F.col("max_least_claimed_amount_approved_amount_check") > 12500.0)
    .drop("max_least_claimed_amount_approved_amount_check")
)

combined_details_adjuster = (
    summary.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.adjuster_id",
        "s.claim_date_month_month",
        "s.policy_premium_2_to_policy_premium_ratio",
        "s.n_rows",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    combined_details_adjuster
    .select(
        "adjuster_id",
        "claim_date_month_month",
        "policy_premium_2_to_policy_premium_ratio",
        "n_rows",
        "adjuster_team",
        "adjuster_level",
    )
    .orderBy(
        F.col("adjuster_team").asc_nulls_last(),
        "adjuster_id",
        "claim_date_month_month",
    )
    .limit(10)
)
