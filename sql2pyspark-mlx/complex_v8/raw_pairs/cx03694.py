from pyspark.sql import functions as F

matched = (
    df_claims.alias("a")
    .join(
        df_claims.alias("b"),
        (F.col("b.policy_id") == F.col("a.policy_id"))
        & (F.col("b.claim_date") < F.col("a.claim_date"))
        & (F.col("b.claim_date") >= F.date_sub(F.col("a.claim_date"), 60)),
        "left",
    )
    .groupBy("a.id", "a.policy_id", "a.claim_date")
    .agg(
        F.count("b.id").alias("prior_rows_60d"),
        F.sum("b.approved_amount").alias("prior_approved_amount_60d"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    matched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.min_claim_claimed_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("reported_at").alias("last_claim_reported_at"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.max("claim_date").alias("last_claim_date"),
    )
)

with_stats = (
    profiled.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.last_claim_reported_at",
        "c.total_claim_approved_amount",
        "c.last_claim_date",
    )
)

result = (
    with_stats
    .select("policy_id", "last_claim_date", "last_claim_reported_at")
    .distinct()
    .orderBy(F.col("last_claim_date").desc())
)
