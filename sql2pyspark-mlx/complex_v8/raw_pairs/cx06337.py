from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.col("policy_id") <= 9) & (F.hour(F.col("reported_at")).between(9, 12)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.abs(F.col("claimed_amount") - 15381.6).alias("claimed_amount_distance"),
    )
)

policy_id_stats = (
    prepared
    .groupBy("policy_id")
    .agg(F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"))
)

compared = (
    prepared.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount") >= F.col("g.median_approved_amount"))
    .select(
        "s.*",
        "g.median_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.median_approved_amount"), 2).alias("approved_amount_minus_median"),
    )
)

rollup_base = (
    compared
    .groupBy(
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
        "status",
        "adjuster_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.sum(F.when(F.col("status") == "OPEN", F.col("median_approved_amount"))).alias("median_approved_amount_status_open"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
        F.round(100 * F.count(F.when(F.col("status").isin("APPROVED", "OPEN"), 1)) / F.count("*"), 1).alias("pct_status_in_approved_open"),
        F.max("claimed_amount_distance").alias("max_claimed_amount_distance_check"),
    )
    .filter(F.col("max_claimed_amount_distance_check") > 10000.0)
    .drop("max_claimed_amount_distance_check")
)

result = (
    rollup_base
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "claim_date_ym",
        "status",
        "adjuster_id",
        "distinct_ids",
        "total_approved_amount",
        "median_approved_amount_status_open",
        "n_status_is_not_null",
        "pct_status_in_approved_open",
        F.round(F.col("total_approved_amount") / F.nullif(F.col("n_status_is_not_null"), F.lit(0)), 3).alias("total_approved_amount_per_n_status_is_not_null"),
    )
)
