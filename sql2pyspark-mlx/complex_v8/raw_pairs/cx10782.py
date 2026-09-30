from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
        F.count("*").alias("claim_count_2"),
    )
)

with_stats = (
    prepared_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.total_claim_approved_amount",
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

policy_id_stats = (
    with_stats
    .groupBy("policy_id")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

compared = (
    with_stats.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

result = (
    compared
    .cube("claim_count_2", "policy_id")
    .agg(
        F.datediff(F.max("claim_date"), F.min("claim_date")).alias("claim_date_span_days"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.countDistinct("status").alias("distinct_status"),
    )
    .orderBy(
        F.col("claim_count_2").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
