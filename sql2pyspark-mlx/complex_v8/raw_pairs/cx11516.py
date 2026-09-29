from pyspark.sql import functions as F

in_window = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 30)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_30d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_30d"),
    )
)

policy_id_stats = (
    in_window
    .groupBy("policy_id")
    .agg(F.percentile_approx("prior_rows_30d", 0.5).alias("median_prior_rows_30d"))
)

vs_group = (
    in_window.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.prior_rows_30d") >= F.col("g.median_prior_rows_30d"))
    .select(
        "s.*",
        "g.median_prior_rows_30d",
        F.round(F.col("s.prior_rows_30d") - F.col("g.median_prior_rows_30d"), 2).alias("prior_rows_30d_minus_median"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

with_counts = (
    vs_group.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

exploded_detail = (
    with_counts.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.prior_rows_30d",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        "c.claim_type",
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "policy_id",
        "prior_rows_30d",
        "claim_status",
        "claim_adjuster_id",
        "claim_type",
        "claim_reported_at",
    )
    .orderBy(F.col("claim_status").desc())
)
