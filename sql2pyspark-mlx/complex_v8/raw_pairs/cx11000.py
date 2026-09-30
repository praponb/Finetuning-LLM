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
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

exploded_detail_claims = (
    joined_ranges.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.product",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

result = (
    exploded_detail_claims
    .filter(F.col("policy_id") <= 3)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "premium",
        "claim_id",
        "claim_type",
        "claim_status",
        "claim_claim_date",
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
    .orderBy(F.col("policy_id").desc(), F.col("premium").desc())
)
