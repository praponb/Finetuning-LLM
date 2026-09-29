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
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

without_matches = (
    covered
    .crossJoin(covered.agg(F.max("premium").alias("overall_max_premium")))
    .filter(F.col("premium") < F.col("overall_max_premium"))
    .drop("overall_max_premium")
)

scored = (
    without_matches
    .select(
        "id",
        "policy_id",
        "status",
        "product",
        "premium",
        (F.floor(F.col("premium") / 1000) * 1000).alias("premium_bucket"),
    )
)

result = (
    scored
    .groupBy("product")
    .pivot("status", ["OPEN", "APPROVED", "IN_REVIEW"])
    .agg(F.avg("premium_bucket"))
    .orderBy("product")
)
