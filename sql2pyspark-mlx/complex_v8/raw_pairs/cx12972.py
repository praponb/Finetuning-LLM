from pyspark.sql import functions as F

base_policys = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2025-12-05").cast("date"), F.lit("2026-12-05").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

line_level_claims = (
    base_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status_slug",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

per_group = (
    line_level_claims
    .filter(F.col("claim_approved_amount") < 15857.9)
    .groupBy("claim_adjuster_id")
    .agg(
        F.max("claim_approved_amount").alias("max_claim_approved_amount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("claim_approved_amount"), 2).alias("total_claim_approved_amount"),
    )
)

result = (
    per_group
    .filter(F.col("distinct_ids") < 7)
    .select(
        "claim_adjuster_id",
        "distinct_ids",
        "total_claim_approved_amount",
        F.abs(F.col("distinct_ids") - 6).alias("distinct_ids_distance"),
    )
    .orderBy(F.col("distinct_ids").desc(), "total_claim_approved_amount")
)
