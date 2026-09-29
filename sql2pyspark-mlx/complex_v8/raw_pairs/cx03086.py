from pyspark.sql import functions as F

scoped_claims = (
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
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
)

with_parent_adjuster = (
    scoped_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_type_len",
        "a.adjuster_name",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    with_parent_adjuster.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

annotated = (
    summarized_rows
    .withColumn("status_group", F.when(F.col("status").isin("IN_REVIEW", "OPEN"), "group_a").when(F.col("status") == "REJECTED", "group_b").otherwise("other"))
)

with_details = (
    annotated.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .filter(F.col("p.premium") >= 1042.8)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_type_len",
        "s.adjuster_name",
        "s.claim_count",
        "s.status_group",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.level").alias("adjuster_level"),
    )
)

adjuster_id_stats = (
    with_details
    .groupBy("adjuster_id")
    .agg(F.avg("policy_premium").alias("avg_policy_premium"))
)

outliers = (
    with_details.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.policy_premium") > F.col("g.avg_policy_premium"))
    .select(
        "s.*",
        "g.avg_policy_premium",
        F.round(F.col("s.policy_premium") - F.col("g.avg_policy_premium"), 2).alias("policy_premium_minus_avg"),
    )
)

result = (
    outliers
    .groupBy("policy_id")
    .pivot("status", ["PAID", "IN_REVIEW", "REJECTED"])
    .agg(F.sum("claim_count"))
    .orderBy("policy_id")
)
