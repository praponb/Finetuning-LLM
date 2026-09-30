from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter((F.col("approved_amount") <= 34401.0) & (F.col("reported_at") < F.lit("2025-02-24 07:00:00").cast("timestamp")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

with_details = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.start_date") < F.lit("2025-05-02").cast("date"))
    .select("s.*", F.col("p.start_date").alias("policy_start_date"))
)

enriched_policy = (
    with_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.claim_type_or_unknown",
        "s.policy_start_date",
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date_2"),
        F.col("po.segment").alias("holder_segment"),
        F.col("po.region").alias("holder_region"),
    )
)

with_activity = (
    enriched_policy.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_type") != "medical"), "left_anti")
    .filter(~F.col("claim_type_or_unknown").isin("theft", "flood"))
)

result = (
    with_activity
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "claim_type_or_unknown",
        "policy_start_date",
        "policy_product",
        "policy_start_date_2",
        "holder_region",
        F.date_add(F.col("claim_date"), 365).alias("claim_date_plus_365d"),
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
    .orderBy(
        F.col("claim_type_slug").asc_nulls_last(),
        F.col("adjuster_id").desc(),
        "id",
    )
    .limit(10)
)
