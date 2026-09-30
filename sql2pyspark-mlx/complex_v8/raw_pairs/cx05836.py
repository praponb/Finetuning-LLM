from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("claimed_amount") >= 10518.4) & (F.col("adjuster_id") >= 4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
    )
)

enriched_adjuster = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.reported_at",
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    enriched_adjuster
    .groupingSets(
        [["adjuster_id", "adjuster_team"], ["adjuster_id"], ["adjuster_team"]],
        "adjuster_id", "adjuster_team",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.min("approved_amount").alias("min_approved_amount"),
        F.sum(F.when(F.col("adjuster_team").isNotNull(), F.col("approved_amount")).otherwise(0)).alias("approved_amount_adjuster_team_is_not_null"),
    )
)
