from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
    )
)

with_children_claims = (
    valid.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        "c.claim_date",
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
    )
)

with_counts = (
    with_children_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.last_claim_reported_at", "c.total_claim_claimed_amount")
)

enriched_adjuster = (
    with_counts.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

totals = (
    enriched_adjuster
    .groupBy(F.date_trunc("month", "claim_date").alias("claim_date_month"))
    .agg(
        F.max("total_claim_claimed_amount").alias("max_total_claim_claimed_amount"),
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
        F.sum("total_claim_claimed_amount").alias("total_total_claim_claimed_amount"),
        F.count("*").alias("cnt"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
    )
)

result = (
    totals
    .select(
        "claim_date_month",
        "max_total_claim_claimed_amount",
        "total_total_claim_claimed_amount",
        "claim_date_span_days",
        F.coalesce(F.col("stddev_claimed_amount"), F.lit(0.0)).alias("stddev_claimed_amount_filled"),
    )
    .orderBy(
        F.col("total_total_claim_claimed_amount").asc(),
        F.col("claim_date_month").desc_nulls_first(),
    )
)
