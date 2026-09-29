from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter(F.col("status") == "PAID")
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
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.initcap(F.trim(F.col("status"))).alias("status_clean"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
)

derived = (
    scoped
    .filter(F.col("claim_type_ref").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "claim_type_status_label",
        "status_clean",
        "claim_type_ref",
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
        F.abs(F.col("claimed_amount") - 28426.9).alias("claimed_amount_distance"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

rollup_base = (
    derived
    .groupBy(
        "policy_id",
        "claim_type",
        F.date_trunc("month", "reported_at").alias("reported_at_month"),
    )
    .agg(
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.max("reported_at").alias("last_reported_at"),
        F.min("claimed_amount").alias("min_claimed_amount"),
        F.max("claimed_amount").alias("max_claimed_amount_check"),
    )
    .filter(F.col("max_claimed_amount_check") > 10518.4)
    .drop("max_claimed_amount_check")
)

enriched = (
    rollup_base
    .select(
        "*",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at_month")).alias("days_since_reported_at_month"),
        F.abs(F.col("distinct_adjuster_ids") - 4).alias("distinct_adjuster_ids_distance"),
    )
)

result = (
    enriched
    .select(
        "policy_id",
        "reported_at_month",
        "distinct_adjuster_ids",
        "last_reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.year(F.col("reported_at_month")).alias("reported_at_month_year"),
    )
)
