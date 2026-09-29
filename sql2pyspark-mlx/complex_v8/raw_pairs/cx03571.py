from pyspark.sql import functions as F

valid = (
    df_claims
    .filter(F.col("status") != "OPEN")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.add_months(F.col("claim_date"), 12).alias("claim_date_plus_12m"),
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
    )
)

with_details = (
    valid.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.premium") >= 1051.9), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_prefix",
        F.col("p.status").alias("policy_status"),
    )
)

derived = (
    with_details
    .select(
        "id",
        "policy_id",
        "reported_at",
        "policy_status",
        F.date_trunc("quarter", F.col("reported_at")).alias("reported_at_quarter"),
        F.coalesce(F.col("policy_status"), F.lit("UNKNOWN")).alias("policy_status_or_unknown"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

combined_details = (
    derived.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.holder_id").isNotNull())
    .select("s.*", F.col("p.holder_id").alias("policy_holder_id"))
)

result = (
    combined_details
    .filter(F.col("policy_holder_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "reported_at",
        "policy_status",
        "reported_at_quarter",
        "policy_status_or_unknown",
        "approved_amount_bucket",
        "policy_holder_id",
        F.concat_ws(" / ", F.col("policy_status"), F.col("policy_status_or_unknown")).alias("policy_status_policy_status_or_unknown_label"),
    )
    .orderBy("policy_holder_id")
)
