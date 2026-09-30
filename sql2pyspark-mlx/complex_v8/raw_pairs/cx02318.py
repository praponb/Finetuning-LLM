from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("claim_reported_at").desc(), F.col("id").desc(), F.col("claim_id").desc())

valid_claims = (
    df_claims
    .filter((~F.col("status").isin("IN_REVIEW", "APPROVED")) & (F.col("policy_id") >= 11))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

line_level_claims = (
    valid_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

deduped = (
    line_level_claims
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_children_claims = (
    deduped.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.claim_id",
        "s.claim_claim_type",
        "s.claim_claimed_amount",
        "s.claim_policy_id",
        F.col("c.id").alias("claim_id_2"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at_2"),
    )
)

enriched = (
    with_children_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "*",
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
        F.coalesce(F.col("claim_claim_type"), F.lit("UNKNOWN")).alias("claim_claim_type_or_unknown"),
    )
)

result = (
    enriched
    .filter(~F.col("claim_claim_type").isin("flood", "collision"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "claim_id",
        "claim_claim_type",
        "claim_claimed_amount",
        "claim_id_2",
        "claim_status",
        "claim_reported_at_2",
        "claim_date_month_start",
        "claim_claim_type_or_unknown",
        (F.col("claim_reported_at_2") + F.expr("INTERVAL 2 HOURS")).alias("claim_reported_at_2_plus_2h"),
        F.when(F.col("approved_amount") >= 14993.0, "A").when(F.col("approved_amount") >= 7204.1, "B").otherwise("C").alias("approved_amount_tier"),
    )
    .orderBy("claim_reported_at_2_plus_2h")
)
