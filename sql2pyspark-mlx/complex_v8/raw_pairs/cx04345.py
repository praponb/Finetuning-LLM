from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("adjuster_id") >= 3))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.*",
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    with_parent
    .groupingSets(
        [["policy_status", "policy_id"], ["policy_status"]],
        "policy_status", "policy_id",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
    )
    .select(
        F.coalesce(F.col("policy_status"), F.lit("ALL")).alias("policy_status"),
        "policy_id",
        "row_count",
        "total_claimed_amount",
    )
    .orderBy(
        F.col("policy_status").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
