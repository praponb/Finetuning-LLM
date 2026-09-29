from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("approved_amount").between(799.5, 38909.4))
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
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
    )
)

claim_type_stats = (
    valid
    .groupBy("claim_type")
    .agg(F.max("claimed_amount").alias("max_claimed_amount"))
)

above_average = (
    valid.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.claimed_amount") == F.col("g.max_claimed_amount"))
    .select(
        "s.*",
        "g.max_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.max_claimed_amount"), 2).alias("claimed_amount_minus_max"),
    )
)

exploded_detail = (
    above_average.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_label",
        "s.claimed_amount_minus_max",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

screened = (
    exploded_detail.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_date") >= F.lit("2024-06-26").cast("date")), "left_anti")
    .filter(F.col("claimed_amount") > 4554.8)
)

result = (
    screened
    .rollup("claim_id", "adjuster_id")
    .agg(
        F.min("claimed_amount_minus_max").alias("min_claimed_amount_minus_max"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount_minus_max"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_minus_max_rati"),
    )
)
