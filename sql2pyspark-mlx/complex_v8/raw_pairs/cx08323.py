from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(6, 10)) & (F.col("policy_id") <= 23))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
)

with_details_adjuster = (
    prepared_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") > 2)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.status_prefix",
        F.col("a.level").alias("adjuster_level"),
    )
)

adjuster_id_stats = (
    with_details_adjuster
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"))
)

compared = (
    with_details_adjuster.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount") >= F.col("g.median_claimed_amount"))
    .select(
        "s.*",
        "g.median_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.median_claimed_amount"), 2).alias("claimed_amount_minus_median"),
    )
)

annotated = (
    compared
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "median_claimed_amount",
        (F.floor(F.col("median_claimed_amount") / 10000) * 10000).alias("median_claimed_amount_bucket"),
        F.lower(F.trim(F.col("status_prefix"))).alias("status_prefix_clean"),
        F.coalesce(F.col("median_claimed_amount"), F.lit(0.0)).alias("median_claimed_amount_filled"),
    )
)

screened = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status").isNotNull()), "left_semi")
)

result = (
    screened
    .filter(F.col("median_claimed_amount").between(2595.0, 45010.0))
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "status",
        "median_claimed_amount_bucket",
        "status_prefix_clean",
        "median_claimed_amount_filled",
    )
    .orderBy(F.col("status").asc(), F.col("id").asc_nulls_last())
)
