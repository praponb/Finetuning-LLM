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

joined = (
    covered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.approved_amount",
        "s.adjuster_id",
        "s.product",
        "s.premium",
        F.col("a.level").alias("adjuster_level"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type") == "collision", 1)).alias("claim_claim_type_count"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claim_claimed_amount"),
    )
)

with_counts = (
    joined.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        "c.avg_claim_claimed_amount",
    )
)

result = (
    with_counts
    .groupingSets(
        [["product", "adjuster_id", "adjuster_level"], ["product"]],
        "product", "adjuster_id", "adjuster_level",
    )
    .agg(
        F.array_sort(F.collect_set("product")).alias("product_list"),
        F.round(F.sum("premium"), 2).alias("total_premium"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
    )
    .orderBy(
        F.col("product").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("adjuster_level").asc_nulls_last(),
    )
)
