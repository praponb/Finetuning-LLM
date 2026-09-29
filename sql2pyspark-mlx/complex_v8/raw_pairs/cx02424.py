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
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

line_level = (
    joined_ranges.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type") != "collision"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.claim_type",
        "s.product",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
    )
)

result = (
    line_level
    .filter(F.col("claim_type").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "product",
        "premium",
        "claim_claim_date",
        "claim_status",
        "claim_claimed_amount",
        F.when(F.col("claim_claim_type").isin("medical", "theft"), "group_a").when(F.col("claim_claim_type") == "delay", "group_b").otherwise("other").alias("claim_claim_type_group"),
    )
)
