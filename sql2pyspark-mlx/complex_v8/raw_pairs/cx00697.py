from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("approved_amount") < 19337.6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

combined_details_adjuster = (
    filtered_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.claimed_amount_bucket",
        "s.greatest_claimed_amount_approved_amount",
        "a.adjuster_name",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    combined_details_adjuster
    .select(
        "id",
        F.col("adjuster_level").alias("adjuster_level_v"),
        F.col("claimed_amount_bucket").alias("claimed_amount_bucket_v"),
    )
    .unpivot(
        ids=["id"],
        values=["adjuster_level_v", "claimed_amount_bucket_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
