from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

joined_adjuster = (
    joined_ranges.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.premium",
        F.col("a.level").alias("adjuster_level"),
    )
)

with_parent = (
    joined_adjuster.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.premium",
        "a.adjuster_name",
        F.col("p.status").alias("policy_status"),
    )
)

annotated = (
    with_parent
    .filter(F.col("adjuster_name").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "premium",
        "adjuster_name",
        "policy_status",
        (F.col("premium") - F.col("claimed_amount")).alias("premium_minus_claimed_amount"),
        F.substring(F.trim(F.col("policy_status")), 1, 3).alias("policy_status_prefix"),
        F.concat_ws("-", F.col("adjuster_name"), F.col("policy_status")).alias("adjuster_name_policy_status_label"),
    )
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "adjuster_name",
        "policy_status",
        "policy_status_prefix",
        "adjuster_name_policy_status_label",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.substring(F.trim(F.col("policy_status")), 1, 2).alias("policy_status_prefix_2"),
    )
    .orderBy(F.col("policy_id").desc(), "id")
    .limit(10)
)
