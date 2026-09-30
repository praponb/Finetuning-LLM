from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("reported_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp")))
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
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
    )
)

enriched = (
    recent_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.status_clean",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

joined_adjuster = (
    enriched.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.status_clean",
        "s.adjuster_name",
        F.col("a.level").alias("adjuster_level_2"),
    )
)

result = (
    joined_adjuster
    .groupingSets(
        [["claim_type", "policy_id"], ["claim_type"], ["policy_id"], []],
        "claim_type", "policy_id",
    )
    .agg(
        F.grouping("claim_type").alias("is_total_claim_type"),
        F.count("*").alias("cnt"),
        F.sum("adjuster_level_2").alias("total_adjuster_level_2"),
        F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0)).alias("n_status_open"),
    )
    .select(
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "policy_id",
        "is_total_claim_type",
        "cnt",
        "total_adjuster_level_2",
        "n_status_open",
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
