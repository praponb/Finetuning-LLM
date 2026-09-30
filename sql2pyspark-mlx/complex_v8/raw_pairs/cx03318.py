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
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

labeled = (
    matched
    .filter(F.col("policy_id") >= 19)
    .select(
        "*",
        F.round(F.col("premium") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("premium_per_approved_amount"),
        F.when(F.col("product").isin("HEALTH", "HOME"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

without_matches = (
    labeled.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claimed_amount") <= 33975.8) & (F.col("c.status") != "OPEN"), "left_semi")
    .filter(~F.col("product_group").isin("group_b", "other"))
)

enriched_policy = (
    without_matches.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.*",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
        F.col("p.product").alias("policy_product"),
    )
)

result = (
    enriched_policy
    .groupBy("product_group", "adjuster_id")
    .pivot("policy_product", ["TRAVEL", "AUTO", "HEALTH", "HOME"])
    .agg(F.min("approved_amount_per_claimed_amount"))
    .orderBy("product_group", "adjuster_id")
)
