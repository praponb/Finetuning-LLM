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
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

labeled = (
    joined_ranges
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.abs(F.col("claimed_amount") - 28755.2).alias("claimed_amount_distance"),
    )
)

result = (
    labeled
    .filter(F.col("claimed_amount_distance") > 20000.0)
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "product",
        "premium",
        "product_slug",
        "claimed_amount_distance",
        F.initcap(F.trim(F.col("product_slug"))).alias("product_slug_clean"),
    )
    .orderBy(F.col("product").desc(), "id")
    .limit(10)
)
