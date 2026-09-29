from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter((~F.col("claim_type").isin("collision", "flood")) & (F.col("claimed_amount") < 41760.8))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 20843.1, "A").when(F.col("approved_amount") >= 1273.7, "B").otherwise("C").alias("approved_amount_tier"),
        F.when(F.col("status").isin("OPEN", "IN_REVIEW"), "group_a").when(F.col("status") == "PAID", "group_b").otherwise("other").alias("status_group"),
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
)

joined_policy = (
    prepared_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select("s.*", F.col("p.product").alias("policy_product"))
)

with_children = (
    joined_policy.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.reported_at",
        "s.status_group",
        "s.status_ref",
        "s.policy_product",
        F.col("c.id").alias("claim_id"),
        F.col("c.claimed_amount").alias("claim_claimed_amount"),
        F.col("c.claim_type").alias("claim_claim_type"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

profiled = (
    with_children.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

claim_stats_2 = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.count("*").alias("claim_count_3"),
    )
)

with_stats = (
    profiled.alias("s")
    .join(claim_stats_2.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_count_3"), F.lit(0)).alias("claim_count_3"),
    )
)

result = (
    with_stats
    .cube("claim_count_3", "adjuster_id", "claim_count")
    .agg(
        F.round(F.avg("claim_count_2"), 2).alias("avg_claim_count_2"),
        F.array_sort(F.collect_set("claim_claim_type")).alias("claim_claim_type_list"),
        F.min("claim_count").alias("min_claim_count"),
    )
)
