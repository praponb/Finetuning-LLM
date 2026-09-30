from pyspark.sql import functions as F

valid = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

tagged = (
    valid
    .select(
        "*",
        F.abs(F.col("claim_date_quarter") - 2).alias("claim_date_quarter_distance"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.round(F.col("status_len") / F.nullif(F.col("claim_date_quarter"), F.lit(0)), 3).alias("status_len_per_claim_date_quarter"),
    )
)

filtered = (
    tagged.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left_semi")
)

with_details_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") < 3)
    .select("s.*", F.col("a.level").alias("adjuster_level"))
)

qualified = (
    with_details_adjuster.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.policy_id") <= 27) & (F.col("c.reported_at") >= F.lit("2025-01-31 14:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("reported_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp"))
)

result = (
    qualified
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "status_len",
        "claim_date_quarter_distance",
        "status_or_unknown",
        "status_len_per_claim_date_quarter",
        "adjuster_level",
    )
    .orderBy(F.col("adjuster_id").desc(), F.col("id").desc())
)
