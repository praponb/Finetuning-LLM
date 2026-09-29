from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.col("claim_date").isNotNull()) | (~F.col("status").isin("REJECTED", "APPROVED")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
    )
)

with_details = (
    recent_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
    )
)

qualified = (
    with_details.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") == "APPROVED") & (F.col("c.claim_type").isNotNull()), "left_anti")
)

combined_details = (
    qualified.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.reported_at",
        "s.adjuster_level",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("a.level").alias("adjuster_level_2"),
    )
)

result = (
    combined_details
    .groupBy("adjuster_level", "claim_date")
    .pivot("claim_type", ["flood", "theft"])
    .agg(F.sum("adjuster_level_2").alias("amount"), F.max("adjuster_level_2").alias("max_v"))
)
