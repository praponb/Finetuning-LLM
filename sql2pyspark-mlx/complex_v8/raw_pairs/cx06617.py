from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

with_details = (
    covered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") <= 3), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.status",
        "s.adjuster_id",
        "s.product",
        "s.premium",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

line_level_claims = (
    with_details.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.status",
        "s.adjuster_id",
        "s.product",
        "s.adjuster_level",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

adjuster_id_stats = (
    line_level_claims
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("adjuster_level", 0.5).alias("median_adjuster_level"))
)

above_average = (
    line_level_claims.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.adjuster_level") >= F.col("g.median_adjuster_level"))
    .select(
        "s.*",
        "g.median_adjuster_level",
        F.round(F.col("s.adjuster_level") - F.col("g.median_adjuster_level"), 2).alias("adjuster_level_minus_median"),
    )
)

result = (
    above_average
    .groupBy("claim_reported_at", "adjuster_id")
    .pivot("status", ["IN_REVIEW", "REJECTED", "APPROVED", "PAID"])
    .agg(F.count("claim_claimed_amount").alias("count_v"), F.max("claim_claimed_amount").alias("max_v"))
    .orderBy("claim_reported_at", "adjuster_id")
)
