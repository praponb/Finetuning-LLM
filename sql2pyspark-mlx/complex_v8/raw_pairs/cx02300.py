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
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

labeled = (
    covered
    .filter(F.col("policy_id") <= 22)
    .select(
        "*",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

result = (
    labeled
    .filter(F.col("product_len") > 6)
    .select(
        "id",
        "claimed_amount",
        "approved_amount",
        "product",
        "premium",
        "product_len",
        "approved_amount_bucket",
        F.date_add(F.col("claim_date"), 30).alias("claim_date_plus_30d"),
    )
    .orderBy(F.col("id").desc())
)
