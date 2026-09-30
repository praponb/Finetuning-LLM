from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_claim_type_group_policy_id_b = Window.partitionBy("claim_type_group", "policy_id").orderBy("approved_amount", "id").rowsBetween(-6, Window.currentRow)

recent_claims = (
    df_claims
    .filter((F.col("approved_amount") >= 0.0) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("approved_amount") + F.col("claimed_amount")).alias("approved_amount_plus_claimed_amount"),
        F.when(F.col("claim_type").isin("collision", "medical"), "group_a").when(F.col("claim_type") == "delay", "group_b").otherwise("other").alias("claim_type_group"),
    )
)

windowed = (
    recent_claims
    .select(
        "*",
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_policy_id), 2).alias("approved_amount_vs_policy_id_avg"),
        F.max("approved_amount").over(w_claim_type_group_policy_id_b).alias("approved_amount_max_7"),
        F.size(F.collect_set("status").over(w_policy_id)).alias("distinct_status_in_policy_id"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

with_stats = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

combined_details_policy = (
    with_stats.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .filter(F.col("p.holder_id").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.approved_amount_plus_claimed_amount",
        "s.approved_amount_vs_policy_id_avg",
        "s.approved_amount_max_7",
        "s.distinct_status_in_policy_id",
        "s.claim_count",
        "s.claim_count_2",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("po.segment").alias("holder_segment"),
        F.col("po.joined_on").alias("holder_joined_on"),
    )
)

with_parent = (
    combined_details_policy.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("s.policy_holder_id"), "inner")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id_2"),
        F.col("p.status").alias("policy_status"),
        F.col("po.segment").alias("holder_segment_2"),
    )
)

result = (
    with_parent
    .filter(F.col("policy_holder_id_2") >= 6)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        "approved_amount_plus_claimed_amount",
        "approved_amount_vs_policy_id_avg",
        "approved_amount_max_7",
        "distinct_status_in_policy_id",
        "claim_count",
        "claim_count_2",
        "policy_holder_id",
        "holder_segment",
        "holder_joined_on",
        "policy_holder_id_2",
        "policy_status",
        "holder_segment_2",
        F.substring(F.trim(F.col("holder_segment_2")), 1, 2).alias("holder_segment_2_prefix"),
        F.when(F.col("holder_segment_2").isin("RETAIL", "VIP"), "group_a").when(F.col("holder_segment_2") == "SME", "group_b").otherwise("other").alias("holder_segment_2_group"),
    )
    .orderBy("approved_amount", "id")
    .limit(20)
)
