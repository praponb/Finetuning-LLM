from pyspark.sql import functions as F

matched = (
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
        "c.status",
        "p.product",
        "p.premium",
    )
)

enriched_policy = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.status",
        "s.premium",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

result = (
    enriched_policy
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "adjuster_id",
        "status",
        "premium",
        "policy_holder_id",
        F.abs(F.col("claimed_amount") - 17585.2).alias("claimed_amount_distance"),
    )
    .orderBy(F.col("premium").asc_nulls_last(), F.col("policy_id").desc())
)
