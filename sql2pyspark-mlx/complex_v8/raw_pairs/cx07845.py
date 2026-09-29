from pyspark.sql import functions as F

cleaned = (
    df_claims
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
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.when(F.col("approved_amount") >= 21750.2, "large").when(F.col("approved_amount") >= 799.5, "medium").otherwise("small").alias("approved_amount_tier"),
        F.round(F.col("approved_amount") * 1.1, 2).alias("approved_amount_adj"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
        F.min("claim_date").alias("first_claim_date"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.total_claim_claimed_amount", "c.first_claim_date")
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.max_claim_approved_amount",
    )
)

combined_details = (
    with_stats.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

result = (
    combined_details
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "approved_amount_adj",
        "total_claim_claimed_amount",
        "first_claim_date",
        "claim_count",
        "max_claim_approved_amount",
        "adjuster_team",
        F.date_trunc("year", F.col("first_claim_date")).alias("first_claim_date_year"),
        F.dayofweek(F.col("first_claim_date")).alias("first_claim_date_dayofweek"),
    )
    .orderBy(F.col("claim_date").desc(), "policy_id", "id")
    .limit(5)
)
