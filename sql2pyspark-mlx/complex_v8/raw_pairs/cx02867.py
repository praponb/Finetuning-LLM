from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("status").isin("OPEN", "PAID", "APPROVED")) & (F.col("claimed_amount").between(6745.1, 42447.2)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
)

with_parent_policy = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.product").isin("HOME", "TRAVEL", "HEALTH"))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_plus_approved_amount",
        "s.reported_at_plus_2h",
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.min("approved_amount").alias("min_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    with_parent_policy.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        "c.min_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count(F.when(F.col("claim_type") == "fire", 1)).alias("claim_claim_type_count"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount_2"),
    )
)

profiled = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.avg_claim_approved_amount_2",
    )
)

result = (
    profiled
    .cube("adjuster_id", "status", "policy_id")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("policy_status")).alias("policy_status_list"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
