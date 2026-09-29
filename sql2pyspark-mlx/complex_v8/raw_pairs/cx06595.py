from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.col("claimed_amount") < 34763.4)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.concat_ws(" / ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

annotated = (
    recent_claims
    .filter(F.col("claim_type_status_label").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "reported_at",
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.when(F.col("status").isin("APPROVED", "IN_REVIEW"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

exploded_detail_claims = (
    annotated.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isin("IN_REVIEW", "PAID")), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount_bucket",
        "s.status_slug",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.status").alias("claim_status"),
    )
)

without_matches = (
    exploded_detail_claims.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()) & (F.col("c.claim_date") < F.lit("2024-09-08").cast("date")), "left_semi")
    .filter(F.col("status_slug").isNotNull())
)

result = (
    without_matches
    .rollup("adjuster_id", "policy_id")
    .agg(
        F.sum(F.when(F.col("claim_status").isin("OPEN", "PAID", "APPROVED"), 1).otherwise(0)).alias("n_claim_status_in_open_paid_ap"),
        F.min("claim_date").alias("first_claim_date"),
    )
)
