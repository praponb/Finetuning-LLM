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
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

qualified = (
    matched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.adjuster_id") <= 4), "left_anti")
    .filter(F.col("policy_id").isNotNull())
)

result = (
    qualified
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "product",
        "premium",
        F.abs(F.col("claimed_amount") - 28435.3).alias("claimed_amount_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
    .orderBy(F.col("policy_id").desc())
)
