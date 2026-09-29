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
        "c.status",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

scored = (
    covered
    .filter(F.col("claim_date") >= F.lit("2024-08-18").cast("date"))
    .withColumn("premium_adj", F.round(F.col("premium") * 1.1, 2))
)

product_stats = (
    scored
    .groupBy("product")
    .agg(F.max("premium_adj").alias("max_premium_adj"))
)

compared = (
    scored.alias("s")
    .join(product_stats.alias("g"), F.col("g.product") == F.col("s.product"), "inner")
    .filter(F.col("s.premium_adj") == F.col("g.max_premium_adj"))
    .select(
        "s.*",
        "g.max_premium_adj",
        F.round(F.col("s.premium_adj") - F.col("g.max_premium_adj"), 2).alias("premium_adj_minus_max"),
    )
)

result = (
    compared
    .filter(F.col("premium").between(986.9, 4688.7))
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "claim_type",
        "premium_adj",
        "premium_adj_minus_max",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.when(F.col("premium_adj") >= 5310.0, "A").when(F.col("premium_adj") >= 2025.0, "B").otherwise("C").alias("premium_adj_tier"),
    )
)
