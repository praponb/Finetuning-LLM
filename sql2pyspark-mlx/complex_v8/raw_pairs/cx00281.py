from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-02-19").cast("date"), F.lit("2025-05-26").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

derived = (
    prepared_claims
    .select(
        "*",
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.month(F.col("claim_date")).alias("claim_date_month"),
    )
)

screened = (
    derived.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.product") == "TRAVEL"), "left_anti")
)

annotated = (
    screened
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "reported_at",
        "status_or_unknown",
        "claim_type_clean",
        "days_since_reported_at",
        F.abs(F.col("days_since_reported_at") - 315).alias("days_since_reported_at_distance"),
    )
)

result = (
    annotated
    .filter(F.col("days_since_reported_at") > 105)
    .select(
        "id",
        "claim_date",
        "reported_at",
        "status_or_unknown",
        "days_since_reported_at",
        "days_since_reported_at_distance",
        F.abs(F.col("claimed_amount") - 29985.9).alias("claimed_amount_distance"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
    .orderBy("days_since_reported_at_distance")
)
