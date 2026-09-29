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
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

qualified = (
    covered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.status").isin("OPEN", "PAID", "REJECTED")), "left_semi")
    .filter(F.col("policy_id").isNotNull())
)

joined = (
    qualified.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select("s.*", F.col("a.team").alias("adjuster_team"), "a.adjuster_name")
)

policy_id_stats = (
    joined
    .groupBy("policy_id")
    .agg(F.avg("premium").alias("avg_premium"))
)

compared = (
    joined.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.premium") > F.col("g.avg_premium"))
    .select(
        "s.*",
        "g.avg_premium",
        F.round(F.col("s.premium") - F.col("g.avg_premium"), 2).alias("premium_minus_avg"),
    )
)

result = (
    compared
    .groupBy("adjuster_id")
    .pivot("claim_type", ["medical", "collision", "flood", "delay"])
    .agg(F.avg("premium"))
)
