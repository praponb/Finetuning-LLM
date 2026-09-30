from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("adjuster_id") >= 7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.abs(F.col("approved_amount") - 14595.6).alias("approved_amount_distance"),
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status_claim_type_label",
        "s.approved_amount_distance",
        "s.claim_date_month_start",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

annotated = (
    with_details
    .withColumn("least_adjuster_level_claimed_amount", F.least(F.col("adjuster_level"), F.col("claimed_amount")))
)

result = (
    annotated
    .groupingSets(
        [["policy_id", "adjuster_id"], ["policy_id"], ["adjuster_id"], []],
        "policy_id", "adjuster_id",
    )
    .agg(
        F.grouping("policy_id").alias("is_total_policy_id"),
        F.max("claim_date_month_start").alias("last_claim_date_month_start"),
    )
)
