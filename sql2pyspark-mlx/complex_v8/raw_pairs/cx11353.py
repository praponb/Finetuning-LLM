from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_policy_id_by_repo = Window.partitionBy("claim_type", "policy_id").orderBy("reported_at_plus_2h", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_type_policy_id_by_repo_2 = Window.partitionBy("claim_type", "policy_id").orderBy("reported_at_plus_2h", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

with_parent_adjuster = (
    cleaned_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("a.team").alias("adjuster_team"),
        F.col("a.level").alias("adjuster_level"),
        F.col("p.status").alias("policy_status"),
    )
)

with_windows = (
    with_parent_adjuster
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "reported_at_plus_2h",
        "greatest_claimed_amount_approved_amount",
        "adjuster_team",
        F.row_number().over(w_claim_type_policy_id_by_repo).alias("rn"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id), 2).alias("adjuster_id_avg_claimed_amount"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.min("approved_amount").over(w_adjuster_id).alias("adjuster_id_min_approved_amount"),
        F.nth_value("adjuster_level", 2).over(w_claim_type_policy_id_by_repo_2).alias("second_adjuster_level"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(~F.col("claim_type").isin("collision", "medical"), 1)).alias("claim_claim_type_count"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["adjuster_team", "second_adjuster_level"], ["adjuster_team"]],
        "adjuster_team", "second_adjuster_level",
    )
    .agg(
        F.round(F.sum("claim_claim_type_count") / F.nullif(F.sum("rn"), F.lit(0)), 3).alias("claim_claim_type_count_to_rn_ratio"),
        F.max("second_adjuster_level").alias("max_second_adjuster_level"),
    )
    .select(
        F.coalesce(F.col("adjuster_team"), F.lit("ALL")).alias("adjuster_team"),
        "second_adjuster_level",
        "claim_claim_type_count_to_rn_ratio",
        "max_second_adjuster_level",
    )
    .orderBy(
        F.col("adjuster_team").asc_nulls_last(),
        F.col("second_adjuster_level").asc_nulls_last(),
    )
)
