from pyspark.sql import functions as F

recent_policys = (
    df_policies
    .filter((F.col("start_date").between(F.lit("2024-10-22").cast("date"), F.lit("2025-07-07").cast("date"))) & (F.col("end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2026-09-19").cast("date"))))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

with_activity = (
    recent_policys.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_type").isin("flood", "fire")) & (F.col("c.adjuster_id").isNotNull()), "left_anti")
)

line_level_claims = (
    with_activity.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.start_date",
        "s.days_end_date_to_start_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_type",
    )
)

detail_rows_claims = (
    line_level_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.approved_amount") >= 0.0), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.days_end_date_to_start_date",
        "s.claim_id",
        "s.claim_approved_amount",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount_2"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

scored = (
    detail_rows_claims
    .select(
        "id",
        "product",
        "days_end_date_to_start_date",
        "claim_id",
        "claim_approved_amount",
        "claim_id_2",
        "claim_claimed_amount",
        "claim_approved_amount_2",
        "claim_adjuster_id",
        (F.col("claim_approved_amount") - F.col("claim_claimed_amount")).alias("claim_approved_amount_minus_claim_claimed_amount"),
        (F.col("claim_approved_amount") + F.col("claim_claimed_amount")).alias("claim_approved_amount_plus_claim_claimed_amount"),
    )
)

result = (
    scored
    .groupBy("claim_id", "claim_id_2")
    .pivot("product", ["LIFE", "TRAVEL", "HEALTH", "AUTO"])
    .agg(F.sum("claim_approved_amount_plus_claim_claimed_amount"))
    .orderBy("claim_id", "claim_id_2")
)
