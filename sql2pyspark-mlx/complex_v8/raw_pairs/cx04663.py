from pyspark.sql import functions as F

covered = (
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
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

enriched = (
    covered
    .select(
        "*",
        F.month(F.col("claim_date")).alias("claim_date_month"),
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.year(F.col("claim_date")).alias("claim_date_year"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.min("reported_at").alias("first_claim_reported_at"))
)

profiled = (
    enriched.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select("s.*", "c.first_claim_reported_at")
)

line_level = (
    profiled.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.adjuster_id",
        "s.premium",
        "s.product_slug",
        "s.first_claim_reported_at",
        F.col("c.id").alias("claim_id"),
        F.col("c.policy_id").alias("claim_policy_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

result = (
    line_level
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "adjuster_id",
        "premium",
        "product_slug",
        "first_claim_reported_at",
        "claim_policy_id",
        "claim_status",
        "claim_reported_at",
        F.concat(F.col("product_slug"), F.lit("#"), F.col("claim_policy_id").cast("string")).alias("product_slug_ref"),
    )
    .orderBy(F.col("policy_id").desc())
)
