from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("claimed_amount") > 4868.7) | (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

scored = (
    cleaned_claims
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "status_ref",
        "claimed_amount_filled",
        F.round(F.col("claimed_amount_filled") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("claimed_amount_filled_per_claimed_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.max("reported_at").alias("last_claim_reported_at"))
)

with_counts = (
    scored.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_reported_at")
)

joined = (
    with_counts.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.upper(F.trim(F.col("a.adjuster_name"))).like("E%")), "inner")
    .select(
        "s.id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status_ref",
        "s.claimed_amount_filled_per_claimed_amount",
        "s.last_claim_reported_at",
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    joined
    .groupingSets(
        [["adjuster_id", "adjuster_level"], ["adjuster_id"], []],
        "adjuster_id", "adjuster_level",
    )
    .agg(
        F.grouping("adjuster_id").alias("is_total_adjuster_id"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("last_claim_reported_at").alias("first_last_claim_reported_at"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("adjuster_level").asc_nulls_last(),
    )
)
