from pyspark.sql import functions as F

joined_ranges = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.count("*").alias("claim_count"),
    )
)

profiled = (
    joined_ranges.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.avg_claim_approved_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

with_children_claims = (
    profiled.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.status",
        "s.product",
        "s.claim_count",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.status").alias("claim_status"),
    )
)

result = (
    with_children_claims
    .select(
        "id",
        "claimed_amount",
        "product",
        "claim_count",
        "claim_id",
        "claim_type",
        "claim_claim_date",
        F.coalesce(F.col("claim_count"), F.lit(0)).alias("claim_count_filled"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
    .orderBy(F.col("claim_type").desc(), F.col("claim_id").desc())
)
