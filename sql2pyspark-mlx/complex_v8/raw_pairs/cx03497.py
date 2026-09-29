from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-04-09").cast("date"), F.lit("2025-04-14").cast("date")))
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
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

status_stats = (
    filtered
    .groupBy("status")
    .agg(F.max("approved_amount").alias("max_approved_amount"))
)

outliers = (
    filtered.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.approved_amount") == F.col("g.max_approved_amount"))
    .select(
        "s.*",
        "g.max_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.max_approved_amount"), 2).alias("approved_amount_minus_max"),
    )
)

with_parent_policy = (
    outliers.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status") == "LAPSED"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.approved_amount_minus_max",
        F.col("p.product").alias("policy_product"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

result = (
    with_parent_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "reported_at",
        "approved_amount_minus_max",
        "policy_product",
        "adjuster_level",
        F.when(F.col("claim_type").isin("theft", "delay"), "group_a").when(F.col("claim_type") == "fire", "group_b").otherwise("other").alias("claim_type_group"),
        F.initcap(F.trim(F.col("policy_product"))).alias("policy_product_clean"),
    )
    .orderBy("policy_id", F.col("policy_product").desc())
)
