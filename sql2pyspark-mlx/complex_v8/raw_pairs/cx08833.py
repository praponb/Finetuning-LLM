from pyspark.sql import functions as F

in_window = (
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
        "c.claim_type",
        "c.status",
        "p.product",
        "p.premium",
    )
)

enriched = (
    in_window.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") < F.lit("2025-03-21").cast("date")), "inner")
    .filter(F.col("p.holder_id") >= 10)
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.claim_type",
        "s.product",
        "s.premium",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

result = (
    enriched
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "claim_type",
        "product",
        "premium",
        "policy_premium",
        "policy_product",
        F.when(F.col("product").isin("HOME", "HEALTH"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
    )
    .orderBy(F.col("product").desc(), F.col("claim_type").asc_nulls_last(), "id")
    .limit(10)
)
