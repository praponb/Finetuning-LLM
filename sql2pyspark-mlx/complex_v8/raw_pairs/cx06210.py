from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-09-14").cast("date")) & (F.col("adjuster_id") <= 3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        F.concat(F.col("status"), F.lit("#"), F.col("id").cast("string")).alias("status_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium").between(986.9, 4678.8)), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status_ref",
        "s.claimed_amount_adj",
        F.col("p.product").alias("policy_product"),
    )
)

with_parent_2 = (
    with_parent.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.end_date").isNotNull()), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.claimed_amount_adj",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("a.team").alias("adjuster_team"),
    )
)

filtered_2 = (
    with_parent_2.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
    .filter(F.col("adjuster_id").isNotNull())
)

result = (
    filtered_2
    .groupingSets(
        [["adjuster_team", "policy_id", "adjuster_id"], ["adjuster_team"], []],
        "adjuster_team", "policy_id", "adjuster_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("claim_type") == "delay", 1)) / F.count("*"), 1).alias("pct_claim_type_delay"),
        F.round(F.stddev("claimed_amount_adj"), 2).alias("stddev_claimed_amount_adj"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
    .select(
        F.coalesce(F.col("adjuster_team"), F.lit("ALL")).alias("adjuster_team"),
        "policy_id",
        "adjuster_id",
        "pct_claim_type_delay",
        "stddev_claimed_amount_adj",
        "distinct_adjuster_ids",
    )
    .orderBy(
        F.col("adjuster_team").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
