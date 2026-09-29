from pyspark.sql import functions as F

recent = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-02-12 12:00:00").cast("timestamp")) & (F.col("adjuster_id") >= 7))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("claim_type").isin("delay", "flood"), "group_a").when(F.col("claim_type") == "collision", "group_b").otherwise("other").alias("claim_type_group"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

annotated = (
    recent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "claim_type_group",
        "reported_at_date",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
)

profiled = (
    annotated.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.avg_claim_approved_amount")
)

with_parent = (
    profiled.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.reported_at",
        "s.claim_type_group",
        "s.reported_at_date",
        "s.claim_date_label",
        "s.last_claim_reported_at",
        "s.avg_claim_approved_amount",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["claim_type_group", "policy_id", "adjuster_id"], ["claim_type_group"], ["policy_id"]],
        "claim_type_group", "policy_id", "adjuster_id",
    )
    .agg(
        F.grouping("claim_type_group").alias("is_total_claim_type_group"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("claim_type_group").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
    )
)
