from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("status") == "REJECTED") & (F.col("approved_amount") < 24718.5))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

with_activity = (
    filtered_claims
    .crossJoin(filtered_claims.agg(F.max("claimed_amount").alias("overall_max_claimed_amount")))
    .filter(F.col("claimed_amount") < F.col("overall_max_claimed_amount"))
    .drop("overall_max_claimed_amount")
)

derived = (
    with_activity
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "reported_at_date",
        "status_len",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

labeled = (
    derived
    .filter(F.col("claim_type_or_unknown").isin("delay", "UNKNOWN", "flood"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "reported_at_date",
        "claim_type_or_unknown",
        F.abs(F.col("status_len") - 6).alias("status_len_distance"),
        F.lower(F.trim(F.col("claim_type_or_unknown"))).alias("claim_type_or_unknown_clean"),
    )
)

without_matches = (
    labeled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") <= 30115.9) & (F.col("c.claim_date").isNotNull()), "left_semi")
)

result = (
    without_matches
    .filter(F.col("reported_at_date") < F.lit("2025-05-01").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "reported_at",
        "reported_at_date",
        "claim_type_or_unknown",
        "status_len_distance",
        "claim_type_or_unknown_clean",
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
        F.date_trunc("year", F.col("reported_at_date")).alias("reported_at_date_year"),
    )
    .orderBy(F.col("reported_at_date_year").asc_nulls_last())
)
