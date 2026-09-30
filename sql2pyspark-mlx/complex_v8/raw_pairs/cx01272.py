from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter(F.col("claimed_amount") >= 3703.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled_2"),
    )
)

with_parent_adjuster = (
    base_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .filter(F.col("a.level") < 3)
    .select(
        "s.*",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
    )
)

result = (
    with_parent_adjuster
    .rollup("adjuster_id", "adjuster_level")
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
    )
)
