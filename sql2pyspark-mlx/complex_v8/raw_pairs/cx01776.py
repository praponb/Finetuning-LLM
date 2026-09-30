from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(7, 9)) & (F.col("claimed_amount") <= 43500.4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"))
)

profiled = (
    base_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

enriched_adjuster = (
    profiled.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Health", "Travel")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_clean",
        "s.approved_amount_bucket",
        "s.claimed_amount_plus_approved_amount",
        "s.claim_count",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

screened = (
    enriched_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_type") == "delay") & (F.col("c.claimed_amount") >= 5313.8), "left_anti")
    .filter(F.col("claim_count") <= 6)
)

result = (
    screened
    .select(
        "id",
        "adjuster_level",
        "claim_count",
        "approved_amount_bucket",
    )
    .unpivot(
        ids=["id"],
        values=["adjuster_level", "claim_count", "approved_amount_bucket"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "measure")
)
