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
        "c.approved_amount",
        "c.status",
        "p.product",
        "p.premium",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("claim_status_count"),
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("claim_count"),
    )
)

summarized_rows = (
    joined_ranges.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "claim_date",
        "approved_amount",
        "status",
        "premium",
        "claim_status_count",
        "last_claim_date",
        F.abs(F.col("claim_status_count") - 3).alias("claim_status_count_distance"),
    )
    .orderBy(F.col("premium").desc())
)
