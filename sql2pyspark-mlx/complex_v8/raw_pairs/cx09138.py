from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("policy_end_date").desc(), F.col("id").desc(), F.col("claim_id").desc())

cleaned_claims = (
    df_claims
    .filter(F.col("status") != "OPEN")
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
        F.round(F.col("claimed_amount") * 1.5, 2).alias("claimed_amount_adj"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
    )
)

detail_rows_claims = (
    cleaned_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.approved_amount").between(1273.7, 39988.0)), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.status",
        "s.claimed_amount_adj",
        "s.status_len",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

labeled = (
    detail_rows_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_claim_date")).alias("days_since_claim_claim_date"),
        (F.col("status_len") + F.col("claim_approved_amount")).alias("status_len_plus_claim_approved_amount"),
    )
)

joined_claim = (
    labeled.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount_adj",
        "s.status_len",
        "s.claim_id",
        "s.claim_approved_amount",
        "s.claim_claim_date",
        "s.status_len_plus_claim_approved_amount",
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

without_matches = (
    joined_claim
    .crossJoin(joined_claim.agg(F.avg("claim_claimed_amount").alias("overall_avg_claim_claimed_amount")))
    .filter(F.col("claim_claimed_amount") > F.col("overall_avg_claim_claimed_amount"))
    .drop("overall_avg_claim_claimed_amount")
)

latest = (
    without_matches
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_parent_adjuster = (
    latest.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount_adj",
        "s.status_len",
        "s.claim_id",
        "s.claim_claim_date",
        "s.status_len_plus_claim_approved_amount",
        "s.claim_claimed_amount",
        "s.policy_end_date",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    with_parent_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount_adj",
        "status_len",
        "claim_id",
        "claim_claim_date",
        "status_len_plus_claim_approved_amount",
        "claim_claimed_amount",
        "policy_end_date",
        "adjuster_team",
        "adjuster_name",
        "adjuster_level",
    )
    .orderBy("claim_type")
)
