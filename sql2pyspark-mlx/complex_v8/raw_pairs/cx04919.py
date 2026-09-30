from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.col("status") != "IN_REVIEW") | (F.col("adjuster_id").isNotNull()))
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
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
    )
)

filtered = (
    recent_claims.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.length(F.trim(F.col("a.adjuster_name"))) >= 6) & (F.col("a.team") == "Health"), "left_semi")
)

result = (
    filtered
    .groupingSets(
        [["status", "claim_type"], ["status"]],
        "status", "claim_type",
    )
    .agg(
        F.grouping("status").alias("is_total_status"),
        F.count("*").alias("n_rows"),
        F.sum("approved_amount").alias("total_approved_amount"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("claim_type").asc_nulls_last())
)
