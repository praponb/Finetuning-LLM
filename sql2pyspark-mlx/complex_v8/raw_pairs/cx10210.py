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

detail_rows = (
    covered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.product",
        F.col("c.id").alias("claim_id"),
        "c.claim_type",
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "product",
        "claim_id",
        "claim_type",
        "claim_approved_amount",
        "claim_reported_at",
        F.length(F.trim(F.col("product"))).alias("product_len"),
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
    )
    .orderBy(F.col("claim_reported_at").desc_nulls_first(), "id")
)
