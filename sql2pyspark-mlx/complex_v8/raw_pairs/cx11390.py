from pyspark.sql import functions as F

joined_ranges = (
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
        "c.approved_amount",
        "c.status",
        "p.product",
        "p.premium",
    )
)

with_children = (
    joined_ranges.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.approved_amount",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

qualified = (
    with_children.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claimed_amount") <= 42447.2) & (F.col("c.claim_type").isNotNull()), "left_semi")
    .filter(F.col("claim_claimed_amount") > 19337.6)
)

result = (
    qualified
    .rollup("claim_id", "claim_status", "policy_id")
    .agg(
        F.min("claim_date").alias("first_claim_date"),
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.count("*").alias("row_count"),
    )
)
