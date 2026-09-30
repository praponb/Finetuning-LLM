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
        "c.approved_amount",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

with_parent = (
    in_window.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.start_date") >= F.lit("2024-10-22").cast("date"))
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.approved_amount",
        "s.claim_type",
        "s.product",
        "s.premium",
        F.col("p.start_date").alias("policy_start_date"),
    )
)

product_stats = (
    with_parent
    .groupBy("product")
    .agg(F.percentile_approx("approved_amount", 0.5).alias("median_approved_amount"))
)

vs_group = (
    with_parent.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.approved_amount") >= F.col("g.median_approved_amount"))
    .select(
        "s.*",
        "g.median_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.median_approved_amount"), 2).alias("approved_amount_minus_median"),
    )
)

result = (
    vs_group
    .filter(F.col("median_approved_amount") <= 30000.0)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "claim_type",
        "premium",
        "policy_start_date",
        "median_approved_amount",
        "approved_amount_minus_median",
        (F.col("median_approved_amount") + F.col("premium")).alias("median_approved_amount_plus_premium"),
    )
    .orderBy("policy_id", "claim_type", "id")
    .limit(5)
)
