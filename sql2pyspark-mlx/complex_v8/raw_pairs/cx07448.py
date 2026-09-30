from pyspark.sql import functions as F

matched = (
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
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

without_matches = (
    matched
    .crossJoin(matched.agg(F.max("claimed_amount").alias("overall_max_claimed_amount")))
    .filter(F.col("claimed_amount") < F.col("overall_max_claimed_amount"))
    .drop("overall_max_claimed_amount")
)

enriched = (
    without_matches.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.product",
        "s.premium",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

totals = (
    enriched
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("row_count"),
        F.max("policy_end_date").alias("last_policy_end_date"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.count(F.when(F.col("product") != "HOME", 1)).alias("n_product_home"),
        F.round(F.avg("claimed_amount"), 2).alias("avg_claimed_amount"),
        F.avg("policy_premium").alias("avg_policy_premium_check"),
    )
    .filter(F.col("avg_policy_premium_check") > 1132.1)
    .drop("avg_policy_premium_check")
)

result = (
    totals
    .select(
        "adjuster_id",
        "row_count",
        "last_policy_end_date",
        "distinct_adjuster_ids",
        "n_product_home",
        "avg_claimed_amount",
    )
    .orderBy(F.col("distinct_adjuster_ids").desc_nulls_first(), "adjuster_id")
    .limit(10)
)
