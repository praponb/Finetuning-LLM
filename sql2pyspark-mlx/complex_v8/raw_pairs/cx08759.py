from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

totals = (
    recent_claims
    .groupBy("status", "adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "flood", 1)).alias("n_claim_type_flood"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
    )
)

enriched_adjuster = (
    totals.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level") >= 3), "inner")
    .select("s.*", "a.adjuster_name", F.col("a.team").alias("adjuster_team"))
)

result = (
    enriched_adjuster
    .select(
        "status",
        "n_claim_type_flood",
        "adjuster_team",
        F.concat_ws("-", F.col("status"), F.col("adjuster_team")).alias("status_adjuster_team_label"),
    )
    .orderBy("adjuster_team")
)
