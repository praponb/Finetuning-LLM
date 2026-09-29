from pyspark.sql import functions as F

matched = (
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
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

enriched = (
    matched.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.claim_type",
        "s.product",
        "s.premium",
        F.col("p.premium").alias("policy_premium"),
        F.col("po.joined_on").alias("holder_joined_on"),
        "po.holder_name",
    )
)

detail_rows_claims = (
    enriched.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.policy_premium",
        "s.holder_joined_on",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_type").alias("claim_claim_type"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

result = (
    detail_rows_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "holder_joined_on",
        "claim_id",
        "claim_approved_amount",
        "claim_claimed_amount",
        "claim_claim_date",
        F.when(F.col("claim_claim_type").isin("medical", "fire"), "group_a").when(F.col("claim_claim_type") == "delay", "group_b").otherwise("other").alias("claim_claim_type_group"),
    )
    .orderBy(F.col("holder_joined_on").asc_nulls_last(), "claim_claimed_amount")
)
