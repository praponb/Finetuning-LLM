from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.col("approved_amount") <= 21750.2) & (F.col("claim_type") != "flood"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

adjuster_id_stats = (
    prepared
    .groupBy("adjuster_id")
    .agg(F.avg("claimed_amount").alias("avg_claimed_amount"))
)

vs_group = (
    prepared.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.claimed_amount") > F.col("g.avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.avg_claimed_amount"), 2).alias("claimed_amount_minus_avg"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.count(F.when(F.col("claim_type").isin("theft", "collision", "flood"), 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    vs_group.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.last_claim_date",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    summarized_rows
    .cube("claim_claim_type_count", "claim_type_len", "policy_id")
    .agg(
        F.grouping("claim_claim_type_count").alias("is_total_claim_claim_type_count"),
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
        F.min("reported_at").alias("first_reported_at"),
        F.round(F.stddev("avg_claimed_amount"), 2).alias("stddev_avg_claimed_amount"),
    )
    .orderBy(
        F.col("claim_claim_type_count").asc_nulls_last(),
        F.col("claim_type_len").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
