from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter(F.col("status") == "OPEN")
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
        F.when(F.col("claimed_amount") >= 29359.4, "HIGH").when(F.col("claimed_amount") >= 11235.6, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier"),
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

exploded_detail = (
    base_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claimed_amount_tier",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

annotated = (
    exploded_detail
    .select(
        "*",
        F.date_trunc("year", F.col("claim_claim_date")).alias("claim_claim_date_year"),
        F.substring(F.trim(F.col("claim_claim_type")), 1, 2).alias("claim_claim_type_prefix"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claim_approved_amount"), F.lit(0)), 3).alias("approved_amount_per_claim_approved_amount"),
    )
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "claimed_amount_tier",
        "claim_id",
        "claim_approved_amount",
        "claim_claim_date",
        F.add_months(F.col("claim_claim_date"), 1).alias("claim_claim_date_plus_1m"),
        F.substring(F.trim(F.col("claim_type")), 1, 2).alias("claim_type_prefix"),
    )
)
