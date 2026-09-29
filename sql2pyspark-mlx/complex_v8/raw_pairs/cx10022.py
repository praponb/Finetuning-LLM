from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

summary = (
    recent_claims
    .groupBy(
        "claim_type",
        "adjuster_id",
        F.date_trunc("month", "reported_at_date").alias("reported_at_date_month"),
    )
    .agg(
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
        F.count("*").alias("row_count"),
        F.max("reported_at").alias("last_reported_at"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.max("reported_at_date").alias("last_reported_at_date"),
        F.sum(F.when(~F.col("claim_type").isin("medical", "fire"), 1).otherwise(0)).alias("n_claim_type_not_in_medical_fi"),
    )
    .filter(F.col("row_count") >= 2)
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("status") != "IN_REVIEW", 1)).alias("claim_status_count"),
        F.max("claim_date").alias("last_claim_date"),
        F.count(F.when(F.col("claim_type").isin("delay", "theft"), 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    summary.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

profiled = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

line_level = (
    profiled.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.claim_type",
        "s.adjuster_id",
        "s.reported_at_date_month",
        "s.median_claimed_amount",
        "s.row_count",
        "s.last_reported_at",
        "s.avg_claimed_amount",
        "s.last_reported_at_date",
        "s.n_claim_type_not_in_medical_fi",
        "s.claim_status_count",
        "s.last_claim_date",
        "s.claim_claim_type_count",
        "s.claim_count",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    line_level
    .select(
        "claim_type",
        "adjuster_id",
        "reported_at_date_month",
        "median_claimed_amount",
        "row_count",
        "avg_claimed_amount",
        "last_reported_at_date",
        "n_claim_type_not_in_medical_fi",
        "claim_status_count",
        "last_claim_date",
        "claim_count",
        "claim_id",
        "claim_policy_id",
        "claim_reported_at",
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
        F.coalesce(F.col("median_claimed_amount"), F.lit(0.0)).alias("median_claimed_amount_filled"),
    )
    .orderBy(F.col("claim_reported_at").desc())
)
