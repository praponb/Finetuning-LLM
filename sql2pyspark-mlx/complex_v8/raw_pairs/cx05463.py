from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(F.col("claim_type") == "theft")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

derived = (
    filtered_claims
    .filter(F.col("policy_id") >= 6)
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status_slug",
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.length(F.trim(F.col("status_slug"))).alias("status_slug_len"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    with_details
    .groupingSets(
        [["claim_type", "policy_holder_id", "status_slug_len"], ["claim_type"], ["policy_holder_id"], []],
        "claim_type", "policy_holder_id", "status_slug_len",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.max("policy_start_date").alias("last_policy_start_date"),
        F.sum("approved_amount").alias("total_approved_amount"),
    )
    .orderBy(
        F.col("claim_type").asc_nulls_last(),
        F.col("policy_holder_id").asc_nulls_last(),
        F.col("status_slug_len").asc_nulls_last(),
    )
)
