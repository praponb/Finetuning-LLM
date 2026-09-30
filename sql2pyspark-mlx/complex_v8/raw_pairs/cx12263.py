from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("claim_type") == "theft") & (F.col("approved_amount") > 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.when(F.col("claim_type").isin("flood", "fire"), "group_a").when(F.col("claim_type") == "medical", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

combined_details_adjuster = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.team") == "Health")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_type_len",
        "s.claim_type_group",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

result = (
    combined_details_adjuster
    .select("id", "approved_amount", "claimed_amount")
    .unpivot(
        ids=["id"],
        values=["approved_amount", "claimed_amount"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
