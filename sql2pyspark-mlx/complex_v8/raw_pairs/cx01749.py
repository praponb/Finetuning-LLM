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
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

with_children_claims = (
    covered.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.approved_amount",
        "s.product",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

enriched = (
    with_children_claims
    .filter(F.col("approved_amount") > 9389.3)
    .select(
        "*",
        F.add_months(F.col("claim_claim_date"), 1).alias("claim_claim_date_plus_1m"),
        F.date_format(F.col("claim_claim_date"), "MMM yyyy").alias("claim_claim_date_label"),
        F.date_trunc("month", F.col("claim_claim_date")).alias("claim_claim_date_month"),
    )
)

result = (
    enriched
    .select(
        "id",
        "approved_amount",
        "product",
        "claim_id",
        "claim_adjuster_id",
        "claim_claimed_amount",
        "claim_claim_date_month",
        F.substring(F.trim(F.col("product")), 1, 4).alias("product_prefix"),
        F.date_format(F.col("claim_claim_date_month"), "yyyy-MM").alias("claim_claim_date_month_label"),
    )
    .orderBy(F.col("claim_claim_date_month").desc())
)
