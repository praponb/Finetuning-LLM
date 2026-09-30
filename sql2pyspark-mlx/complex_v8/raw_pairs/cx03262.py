from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_adjuster_level = Window.partitionBy("adjuster_level")
w_claim_type = Window.partitionBy("claim_type")
w_policy_id_adjuster_level_by = Window.partitionBy("policy_id", "adjuster_level").orderBy(F.col("adjuster_level").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_reported_at_year_by_c = Window.partitionBy("status", F.year("reported_at")).orderBy("claimed_amount")
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy("reported_at", "id")
w_claim_type_policy_id_by_repo = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("reported_at").desc(), "id")
w_all_by_claimed_amount_full = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id = Window.partitionBy("policy_id")

recent = (
    df_claims
    .filter((F.to_date(F.col("reported_at")) < F.lit("2025-02-12").cast("date")) & (F.col("claim_type") == "fire"))
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
    )
)

combined_details_adjuster = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level") <= 3)
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

windowed = (
    combined_details_adjuster
    .select(
        "*",
        F.size(F.collect_set("claim_type").over(w_status)).alias("distinct_claim_type_in_status"),
        F.count(F.lit(1)).over(w_adjuster_level).alias("rows_in_adjuster_level"),
        F.size(F.collect_set("policy_id").over(w_claim_type)).alias("distinct_policy_id_in_claim_type"),
        F.max("adjuster_level").over(w_policy_id_adjuster_level_by).alias("max_adjuster_level_so_far"),
        F.round(F.percent_rank().over(w_status_reported_at_year_by_c), 3).alias("claimed_amount_percent_rank"),
        F.round(F.percent_rank().over(w_claim_type_by_reported_at), 3).alias("reported_at_percent_rank"),
        F.sum("adjuster_level").over(w_claim_type).alias("claim_type_sum_adjuster_level"),
        F.lead("claimed_amount").over(w_claim_type_policy_id_by_repo).alias("next_claimed_amount"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.nth_value("reported_at_percent_rank", 2).over(w_all_by_claimed_amount_full).alias("second_reported_at_percent_rank"),
        F.max("reported_at_percent_rank").over(w_policy_id).alias("policy_id_max_reported_at_percent_rank"),
        F.first("next_claimed_amount").over(w_all_by_claimed_amount_full).alias("first_next_claimed_amount"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id)).alias("distinct_adjuster_id_in_policy_id"),
        F.sum("reported_at_percent_rank").over(w_policy_id).alias("policy_id_sum_reported_at_percent_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "adjuster_name",
        "adjuster_level",
        "distinct_claim_type_in_status",
        "rows_in_adjuster_level",
        "distinct_policy_id_in_claim_type",
        "max_adjuster_level_so_far",
        "reported_at_percent_rank",
        "claim_type_sum_adjuster_level",
        "next_claimed_amount",
        "second_reported_at_percent_rank",
        "distinct_adjuster_id_in_policy_id",
        (F.col("claimed_amount") + F.col("next_claimed_amount")).alias("claimed_amount_plus_next_claimed_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.col("reported_at_percent_rank"), F.lit(0)), 3).alias("approved_amount_per_reported_at_percent_rank"),
    )
)
