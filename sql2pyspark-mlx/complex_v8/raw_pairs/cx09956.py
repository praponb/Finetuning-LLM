from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("claim_type") == "medical") & (F.col("claimed_amount") > 18924.4))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
    )
)

combined_details = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
    )
)

derived = (
    combined_details
    .filter(F.col("adjuster_id") <= 3)
    .select(
        "*",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

labeled = (
    derived
    .select(
        "*",
        F.concat(F.col("claim_type_or_unknown"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_or_unknown_ref"),
        F.concat_ws(" / ", F.col("policy_status"), F.col("claim_type")).alias("policy_status_claim_type_label"),
    )
)

result = (
    labeled
    .rollup("policy_id", "claim_type_or_unknown", "policy_product")
    .agg(
        F.min("claimed_amount").alias("min_claimed_amount"),
        F.sum(F.when(F.col("claim_type") != "fire", F.col("claimed_amount")).otherwise(0)).alias("claimed_amount_claim_type_fire"),
        F.max("claim_date").alias("last_claim_date"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("claim_type_or_unknown").asc_nulls_last(),
        F.col("policy_product").asc_nulls_last(),
    )
)
