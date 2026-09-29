from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_cnt_distinct_policy_ids_buck = Window.partitionBy("cnt", "distinct_policy_ids_bucket")

filtered = (
    df_claims
    .filter(F.col("approved_amount") > 492.6)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.round(F.col("claimed_amount") * 0.9, 2).alias("claimed_amount_adj"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
)

policy_id_stats = (
    filtered
    .groupBy("policy_id")
    .agg(F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"))
)

vs_group = (
    filtered.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.claimed_amount") >= F.col("g.median_claimed_amount"))
    .select(
        "s.*",
        "g.median_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.median_claimed_amount"), 2).alias("claimed_amount_minus_median"),
    )
)

rollup_base = (
    vs_group
    .groupBy("claim_type_len")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.count("*").alias("cnt"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
    )
)

labeled = (
    rollup_base
    .withColumn("distinct_policy_ids_bucket", F.floor(F.col("distinct_policy_ids") / 2) * 2)
)

ranked = (
    labeled
    .withColumn("cnt_distinct_policy_ids_bucket_max_distinct_stat", F.max("distinct_status").over(w_cnt_distinct_policy_ids_buck))
    .withColumn("cnt_distinct_policy_ids_bucket_avg_distinct_stat", F.round(F.avg("distinct_status").over(w_cnt_distinct_policy_ids_buck), 2))
)

scored = (
    ranked
    .select(
        "claim_type_len",
        "distinct_policy_ids",
        "cnt_distinct_policy_ids_bucket_max_distinct_stat",
        "cnt_distinct_policy_ids_bucket_avg_distinct_stat",
        F.abs(F.col("distinct_policy_ids") - 5).alias("distinct_policy_ids_distance"),
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
        (F.col("distinct_status") + F.col("claim_type_len")).alias("distinct_status_plus_claim_type_len"),
    )
)

result = (
    scored
    .select(
        "claim_type_len",
        "cnt_distinct_policy_ids_bucket_max_distinct_stat",
        "cnt_distinct_policy_ids_bucket_avg_distinct_stat",
        "distinct_policy_ids_distance",
        "cnt_bucket",
        "distinct_status_plus_claim_type_len",
        (F.col("cnt_bucket") + F.col("distinct_policy_ids")).alias("cnt_bucket_plus_distinct_policy_ids"),
    )
    .orderBy(
        F.col("cnt_distinct_policy_ids_bucket_avg_distinct_stat").desc(),
        F.col("distinct_policy_ids_distance").asc_nulls_last(),
    )
)
