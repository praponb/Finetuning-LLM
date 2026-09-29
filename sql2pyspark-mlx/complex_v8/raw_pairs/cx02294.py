from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.col("claimed_amount") >= 14019.8) & (F.col("adjuster_id") >= 4))
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
    )
)

scored = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.date_format(F.col("claim_date"), "yyyy-MM").alias("claim_date_label"),
    )
)

tagged = (
    scored
    .select(
        "*",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

policy_id_stats = (
    tagged
    .groupBy("policy_id")
    .agg(F.max("days_since_claim_date").alias("max_days_since_claim_date"))
)

above_average = (
    tagged.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.days_since_claim_date") == F.col("g.max_days_since_claim_date"))
    .select(
        "s.*",
        "g.max_days_since_claim_date",
        F.round(F.col("s.days_since_claim_date") - F.col("g.max_days_since_claim_date"), 2).alias("days_since_claim_date_minus_max"),
    )
)

result = (
    above_average
    .rollup("status_len", "claim_type")
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
    )
    .orderBy(
        F.col("status_len").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
