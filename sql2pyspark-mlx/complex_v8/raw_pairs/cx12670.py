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

with_children = (
    covered.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.approved_amount",
        "s.adjuster_id",
        "s.product",
        F.col("c.id").alias("claim_id"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.status").alias("claim_status"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.claim_date").alias("claim_claim_date"),
    )
)

claim_status_stats = (
    with_children
    .groupBy("claim_status")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

above_average = (
    with_children.alias("s")
    .join(claim_status_stats.alias("g"), F.col("g.claim_status") == F.col("s.claim_status"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "product",
        "claim_id",
        "claim_reported_at",
        "claim_claimed_amount",
        "claim_claim_date",
        "avg_approved_amount",
        F.concat_ws("|", F.col("claim_status"), F.col("product")).alias("claim_status_product_label"),
    )
    .orderBy(F.col("claim_claimed_amount").desc())
)
