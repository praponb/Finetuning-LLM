from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type = Window.partitionBy("claim_type")
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_all_by_approved_amount_full = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_approved_amount_dense_rank = Window.partitionBy("approved_amount_dense_rank")
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy("claim_type_max_claimed_amount", "id")
w_all_by_approved_amount_dense = Window.orderBy("approved_amount_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount_dense_2 = Window.orderBy("approved_amount_dense_rank")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy(F.col("claimed_amount_minus_max").desc_nulls_first())

scoped_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 4) & (F.col("claim_type").isin("fire", "flood", "theft")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

windowed = (
    scoped_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.max("claimed_amount").over(w_claim_type).alias("claim_type_max_claimed_amount"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
    )
)

windowed_2 = (
    windowed
    .withColumn("approved_amount_dense_rank", F.dense_rank().over(w_all_by_approved_amount_ties))
    .withColumn("last_claimed_amount", F.last("claimed_amount").over(w_all_by_approved_amount_full))
)

policy_id_stats = (
    windowed_2
    .groupBy("policy_id")
    .agg(F.max("claimed_amount").alias("max_claimed_amount"))
)

above_average = (
    windowed_2.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.claimed_amount") == F.col("g.max_claimed_amount"))
    .select(
        "s.*",
        "g.max_claimed_amount",
        F.round(F.col("s.claimed_amount") - F.col("g.max_claimed_amount"), 2).alias("claimed_amount_minus_max"),
    )
)

with_windows = (
    above_average
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "rows_in_claim_type",
        "max_claimed_amount",
        "claimed_amount_minus_max",
        F.round(F.col("last_claimed_amount") - F.avg("last_claimed_amount").over(w_approved_amount_dense_rank), 2).alias("last_claimed_amount_vs_approved_amount_dense_ran"),
        F.lead("approved_amount", 3).over(w_claim_type_policy_id_by_clai).alias("next3_approved_amount"),
        F.first("approved_amount_dense_rank").over(w_all_by_approved_amount_dense).alias("first_approved_amount_dense_rank"),
        F.rank().over(w_all_by_approved_amount_dense_2).alias("approved_amount_dense_rank_rank"),
        F.round(F.percent_rank().over(w_claim_type_by_claimed_amount), 3).alias("claimed_amount_minus_max_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_approved_amount_dense_2), 3).alias("approved_amount_dense_rank_percent_rank"),
    )
)

detail_rows_claims = (
    with_windows.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claim_type",
        "s.max_claimed_amount",
        "s.claimed_amount_minus_max",
        "s.last_claimed_amount_vs_approved_amount_dense_ran",
        "s.first_approved_amount_dense_rank",
        "s.approved_amount_dense_rank_percent_rank",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
    )
)

with_details_policy = (
    detail_rows_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claim_type",
        "s.max_claimed_amount",
        "s.claimed_amount_minus_max",
        "s.last_claimed_amount_vs_approved_amount_dense_ran",
        "s.first_approved_amount_dense_rank",
        "s.approved_amount_dense_rank_percent_rank",
        "s.claim_id",
        "s.claim_claim_date",
        "s.claim_reported_at",
        "s.claim_approved_amount",
        "s.claim_adjuster_id",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
    )
)

result = (
    with_details_policy
    .cube("claim_adjuster_id", "first_approved_amount_dense_rank", "claim_type")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(100 * F.count(F.when(~F.col("policy_status").isin("CANCELLED", "EXPIRED"), 1)) / F.count("*"), 1).alias("pct_policy_status_not_in_cancell"),
    )
    .select(
        "claim_adjuster_id",
        "first_approved_amount_dense_rank",
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "gid",
        "pct_policy_status_not_in_cancell",
    )
    .orderBy(
        F.col("claim_adjuster_id").asc_nulls_last(),
        F.col("first_approved_amount_dense_rank").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
