from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("policy_id") <= 24) & (F.col("adjuster_id").isNotNull()))
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
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.date_add(F.col("claim_date"), 30).alias("claim_date_plus_30d"),
    )
)

with_activity = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id").isNotNull()) & (F.col("c.reported_at") < F.lit("2025-02-12 14:00:00").cast("timestamp")), "left_anti")
)

exploded_detail_claims = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.status_slug",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

totals = (
    exploded_detail_claims
    .filter(F.col("policy_id") >= 7)
    .groupBy(
        F.date_format("claim_reported_at", "yyyy-MM").alias("claim_reported_at_ym"),
        "adjuster_id",
        "status",
    )
    .agg(
        F.sum(F.when(F.col("claim_claim_type") == "collision", F.col("claimed_amount")).otherwise(0)).alias("claimed_amount_claim_claim_type_collision"),
        F.round(F.sum("claim_approved_amount") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("claim_approved_amount_to_claimed_amount_ratio"),
        F.count("*").alias("row_count"),
        F.min("claimed_amount").alias("min_claimed_amount"),
        F.countDistinct("status").alias("distinct_status"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
    .filter(F.col("row_count") >= 2)
)

screened = (
    totals.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isin("APPROVED", "PAID", "IN_REVIEW")), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

profiled = (
    screened.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

detail_rows = (
    profiled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.to_date(F.col("c.reported_at")) < F.lit("2025-03-08").cast("date")), "inner")
    .select(
        "s.claim_reported_at_ym",
        "s.adjuster_id",
        "s.status",
        "s.min_claimed_amount",
        "s.distinct_claim_types",
        "s.claim_count",
        "s.claim_count_2",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

result = (
    detail_rows
    .filter(F.col("claim_id").isNotNull())
    .select(
        "claim_reported_at_ym",
        "adjuster_id",
        "min_claimed_amount",
        "distinct_claim_types",
        "claim_count",
        "claim_id",
        "claim_reported_at",
        "claim_type",
        "claim_approved_amount",
        "claim_policy_id",
    )
    .orderBy(F.col("adjuster_id").desc(), F.col("claim_approved_amount").desc())
)
