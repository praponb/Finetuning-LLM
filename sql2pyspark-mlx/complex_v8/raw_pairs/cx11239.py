from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_claim_type_coun = Window.orderBy(F.col("claim_claim_type_count").desc_nulls_first())
w_adjuster_team_by_approved_am = Window.partitionBy("adjuster_team").orderBy(F.col("approved_amount_bucket").desc_nulls_first(), "id")
w_all_by_claim_claim_type_coun_2 = Window.orderBy(F.col("claim_claim_type_count").desc_nulls_first(), "id")

base = (
    df_claims
    .filter((F.col("claim_type") == "flood") & (F.col("claimed_amount").between(4698.6, 39666.5)))
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
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

derived = (
    summarized_rows
    .select(
        "*",
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
)

joined_adjuster = (
    derived.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.reported_at",
        "s.claim_type_len",
        "s.approved_amount_bucket",
        "s.days_since_reported_at",
        "s.claim_claim_type_count",
        "s.claim_type_prefix",
        "s.status_prefix",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
        F.col("p.premium").alias("policy_premium"),
    )
)

sequenced = (
    joined_adjuster
    .select(
        "*",
        F.rank().over(w_all_by_claim_claim_type_coun).alias("claim_claim_type_count_rank"),
        F.ntile(10).over(w_adjuster_team_by_approved_am).alias("approved_amount_bucket_decile"),
        F.dense_rank().over(w_all_by_claim_claim_type_coun_2).alias("claim_claim_type_count_dense_rank"),
    )
)

with_details_policy = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .filter(F.col("p.product") == "HEALTH")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.reported_at",
        "s.claim_claim_type_count",
        "s.status_prefix",
        "s.adjuster_level",
        "s.adjuster_name",
        "s.policy_premium",
        "s.approved_amount_bucket_decile",
        "s.claim_claim_type_count_dense_rank",
        F.col("p.product").alias("policy_product"),
    )
)

summary = (
    with_details_policy
    .groupBy(
        "approved_amount_bucket_decile",
        "claim_claim_type_count",
        "policy_product",
    )
    .agg(
        F.percentile_approx("claim_claim_type_count", 0.5).alias("median_claim_claim_type_count"),
        F.round(F.avg("claim_claim_type_count_dense_rank"), 2).alias("avg_claim_claim_type_count_dense_rank"),
        F.count("*").alias("cnt"),
        F.min("adjuster_level").alias("min_adjuster_level"),
        F.round(F.sum("claim_claim_type_count_dense_rank") / F.nullif(F.sum("claim_claim_type_count"), F.lit(0)), 3).alias("claim_claim_type_count_dense_rank_to_claim_claim"),
        F.avg("claim_claim_type_count").alias("avg_claim_claim_type_count_check"),
    )
    .filter(F.col("avg_claim_claim_type_count_check") > 2)
    .drop("avg_claim_claim_type_count_check")
)

result = (
    summary
    .select(
        "approved_amount_bucket_decile",
        "claim_claim_type_count",
        "policy_product",
        "median_claim_claim_type_count",
        "avg_claim_claim_type_count_dense_rank",
        "cnt",
        "min_adjuster_level",
        "claim_claim_type_count_dense_rank_to_claim_claim",
    )
    .orderBy(
        F.col("approved_amount_bucket_decile").desc(),
        F.col("avg_claim_claim_type_count_dense_rank").desc_nulls_first(),
    )
)
