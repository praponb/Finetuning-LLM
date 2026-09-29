from pyspark.sql import functions as F

covered = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"))
)

summarized_rows = (
    covered.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "claim_type",
        "premium",
        "claim_count",
    )
    .orderBy(F.col("premium").desc(), F.col("policy_id").desc())
)
