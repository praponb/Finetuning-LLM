from pyspark.sql import functions as F

valid = (
    df_claims
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
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.claim_date_label",
        "s.claim_type_or_unknown",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    with_details
    .groupBy("status", "reported_at")
    .pivot("claim_type_or_unknown", ["flood", "theft", "delay", "UNKNOWN"])
    .agg(F.count("claimed_amount"))
    .orderBy("status", "reported_at")
)
