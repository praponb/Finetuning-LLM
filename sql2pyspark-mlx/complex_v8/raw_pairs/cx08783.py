from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_product_policy_holder = Window.partitionBy("policy_product", "policy_holder_id").orderBy("claim_date")
w_policy_id_policy_holder_id = Window.partitionBy("policy_id", "policy_holder_id")

valid = (
    df_claims
    .filter(F.col("reported_at") < F.lit("2025-02-18 09:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "reported_at",
    )
)

enriched_policy = (
    valid.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.product").alias("policy_product"),
    )
)

metrics = (
    enriched_policy
    .select(
        "*",
        F.round(F.cume_dist().over(w_policy_product_policy_holder), 3).alias("claim_date_cume_dist"),
        F.round(F.percent_rank().over(w_policy_product_policy_holder), 3).alias("claim_date_percent_rank"),
        F.size(F.collect_set("policy_product").over(w_policy_id_policy_holder_id)).alias("distinct_policy_product_in_policy_id_policy_hold"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(F.min("claim_date").alias("first_claim_date"))
)

summarized_rows = (
    metrics.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "c.first_claim_date")
)

joined_adjuster = (
    summarized_rows.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.policy_start_date",
        "s.policy_holder_id",
        "s.policy_product",
        "s.claim_date_cume_dist",
        "s.claim_date_percent_rank",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

combined_details = (
    joined_adjuster.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.policy_start_date",
        "s.policy_holder_id",
        "s.policy_product",
        "s.claim_date_cume_dist",
        "s.claim_date_percent_rank",
        "s.adjuster_name",
        F.col("a.adjuster_name").alias("adjuster_adjuster_name"),
        F.col("a.level").alias("adjuster_level_2"),
        F.col("a.team").alias("adjuster_team"),
        F.col("p.status").alias("policy_status"),
    )
)

with_details = (
    combined_details.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select("s.*", F.col("a.adjuster_name").alias("adjuster_adjuster_name_2"))
)

result = (
    with_details
    .filter(F.upper(F.trim(F.col("adjuster_name"))).like("H%"))
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "policy_holder_id",
        "policy_product",
        "claim_date_percent_rank",
        "adjuster_adjuster_name",
        "adjuster_level_2",
        "adjuster_team",
        "policy_status",
    )
    .orderBy(F.col("claim_date_percent_rank").asc(), "adjuster_level_2", "id")
    .limit(20)
)
