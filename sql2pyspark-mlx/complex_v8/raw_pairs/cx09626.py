from pyspark.sql import functions as F

in_window = (
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

labeled = (
    in_window
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "claim_date",
        "status",
        "adjuster_id",
        "product",
        "premium",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

result = (
    labeled
    .select(
        "id",
        "status",
        "adjuster_id",
        "status_ref",
        F.lower(F.regexp_replace(F.trim(F.col("status_ref")), " ", "_")).alias("status_ref_slug"),
        F.concat(F.col("product"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("product_ref"),
    )
    .orderBy(F.col("status").asc(), "status_ref", "id")
    .limit(10)
)
