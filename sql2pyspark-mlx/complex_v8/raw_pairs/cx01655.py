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
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

qualified = (
    joined_ranges.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") >= F.lit("2024-04-03").cast("date")), "left_semi")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
    )
)

summarized_rows = (
    qualified.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.max_claim_claimed_amount",
        "c.avg_claim_approved_amount",
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.max("claim_date").alias("last_claim_date_2"),
        F.max("approved_amount").alias("max_claim_approved_amount"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        "c.last_claim_date_2",
        "c.max_claim_approved_amount",
    )
)

result = (
    with_stats
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "approved_amount",
        "product",
        "premium",
        "claim_count",
        "max_claim_claimed_amount",
        "last_claim_date_2",
        "max_claim_approved_amount",
    )
    .orderBy(F.col("claim_count").desc())
)
