from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter(F.col("claim_date") >= F.lit("2024-08-10").cast("date"))
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
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

line_level_claims = (
    prepared.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_type") != "flood"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.days_since_claim_date",
        "s.claim_type_status_label",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    line_level_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.total_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

filtered = (
    profiled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.claimed_amount") <= 40638.4), "left_anti")
)

result = (
    filtered
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "days_since_claim_date",
        "claim_type_status_label",
        "claim_id",
        "claim_approved_amount",
        "claim_claimed_amount",
        "claim_claim_type",
        "total_claim_approved_amount",
        "claim_count",
        F.concat_ws(": ", F.col("status"), F.col("claim_claim_type")).alias("status_claim_claim_type_label"),
        F.abs(F.col("claimed_amount") - 32796.4).alias("claimed_amount_distance"),
    )
    .orderBy(F.col("claimed_amount_distance").desc_nulls_first())
)
