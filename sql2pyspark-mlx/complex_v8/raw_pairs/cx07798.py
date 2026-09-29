from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(F.hour(F.col("reported_at")).between(9, 11))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(~F.col("status").isin("APPROVED", "OPEN"), 1)).alias("claim_status_count"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("claim_status_count_2"),
    )
)

summarized_rows = (
    recent_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        F.coalesce(F.col("c.claim_status_count_2"), F.lit(0)).alias("claim_status_count_2"),
    )
)

tagged = (
    summarized_rows
    .filter(F.year(F.col("claim_date")) == 2025)
    .select(
        "*",
        F.round(F.col("claim_status_count") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claim_status_count_per_approved_amount"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("claim_status_count_2"), F.lit(0)), 3).alias("claimed_amount_per_claim_status_count_2"),
        F.lower(F.regexp_replace(F.trim(F.col("reported_at_label")), " ", "_")).alias("reported_at_label_slug"),
    )
)

result = (
    tagged
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "reported_at_label",
        "claim_status_count",
        "claim_status_count_2",
        "claim_status_count_per_approved_amount",
        "claimed_amount_per_claim_status_count_2",
        "reported_at_label_slug",
    )
    .orderBy(F.col("claim_status_count").desc(), F.col("claimed_amount").asc())
)
