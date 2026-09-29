from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("claim_date") >= F.lit("2024-06-27").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
)

with_stats = (
    valid.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.avg_claim_approved_amount",
    )
)

combined_details_policy = (
    with_stats.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .filter(F.col("p.holder_id") >= 4)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.status_clean",
        "s.reported_at_label",
        "s.claim_count",
        "s.avg_claim_approved_amount",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.status").alias("policy_status"),
        F.col("po.region").alias("holder_region"),
    )
)

enriched = (
    combined_details_policy
    .filter(F.col("policy_holder_id").isNotNull())
    .select(
        "*",
        F.concat(F.col("policy_status"), F.lit("#"), F.col("id").cast("string")).alias("policy_status_ref"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

result = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status_clean",
        "reported_at_label",
        "claim_count",
        "avg_claim_approved_amount",
        "policy_holder_id",
        "policy_status",
        "holder_region",
        F.lower(F.trim(F.col("holder_region"))).alias("holder_region_clean"),
    )
    .orderBy(F.col("claim_count").desc(), "policy_holder_id")
)
