from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter(~F.col("status").isin("PAID", "IN_REVIEW"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    filtered_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

without_matches = (
    summarized_rows.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id").isNotNull()), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "status_clean",
        "first_claim_reported_at",
        "claim_count",
        F.date_format(F.col("first_claim_reported_at"), "yyyy-MM-dd").alias("first_claim_reported_at_label"),
        F.date_format(F.col("first_claim_reported_at"), "yyyy-MM-dd").alias("first_claim_reported_at_label_2"),
    )
    .orderBy(F.col("first_claim_reported_at_label_2").desc())
)
