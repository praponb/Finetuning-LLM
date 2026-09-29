from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
        F.dayofweek(F.col("end_date")).alias("end_date_dayofweek"),
        F.when(F.col("product").isin("HEALTH", "TRAVEL"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
        F.when(F.col("status").isin("EXPIRED", "CANCELLED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") < 0.8), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.product_group",
        "s.status_group",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        "p.holder_name",
    )
)

with_parent = (
    joined.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        "s.product_group",
        "s.status_group",
        "s.holder_segment",
        "s.holder_region",
        "s.holder_name",
        F.col("p.region").alias("holder_region_2"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("claim_date").alias("first_claim_date"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
    )
)

with_stats = (
    with_parent.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_date",
        "c.total_claim_approved_amount",
    )
)

exploded_detail = (
    with_stats.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") < 31715.7), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.end_date",
        "s.status_group",
        "s.holder_region_2",
        "s.holder_risk_score",
        "s.claim_count",
        "s.first_claim_date",
        "s.total_claim_approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        "c.claim_type",
        "c.claim_date",
    )
)

with_details = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "left")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status_group",
        "s.holder_region_2",
        "s.claim_count",
        "s.first_claim_date",
        "s.total_claim_approved_amount",
        "s.claim_id",
        "s.claim_status",
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("p.segment").alias("holder_segment"),
    )
)

screened = (
    with_details.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isin("ACTIVE", "EXPIRED")) & (F.col("p.start_date") >= F.lit("2025-01-05").cast("date")), "left_anti")
)

result = (
    screened
    .filter(F.col("claim_claim_date") < F.lit("2024-12-20").cast("date"))
    .select(
        "id",
        "status_group",
        "holder_region_2",
        "claim_count",
        "claim_status",
        "claim_claim_date",
        "claim_reported_at",
        "holder_segment",
    )
    .orderBy(F.col("claim_status").desc())
)
