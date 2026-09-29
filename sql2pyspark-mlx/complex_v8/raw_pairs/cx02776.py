from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("adjuster_id") <= 2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

profiled = (
    recent_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.last_claim_date",
    )
)

annotated = (
    profiled
    .select(
        "id",
        "adjuster_id",
        "reported_at",
        "claim_count",
        "last_claim_date",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
)

with_activity = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_type").isNotNull()) & (F.col("c.reported_at") < F.lit("2025-02-18 15:00:00").cast("timestamp")), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "adjuster_id",
        "reported_at",
        "claim_count",
        "last_claim_date",
        "status_or_unknown",
        "reported_at_plus_2h",
        F.lower(F.regexp_replace(F.trim(F.col("status_or_unknown")), " ", "_")).alias("status_or_unknown_slug"),
        F.coalesce(F.col("status_or_unknown"), F.lit("UNKNOWN")).alias("status_or_unknown_or_unknown"),
    )
)
