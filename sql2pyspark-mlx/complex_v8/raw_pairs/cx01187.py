from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_team_claim_type_by = Window.partitionBy("adjuster_team", "claim_type").orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_claim_type_adjuster_team_by = Window.partitionBy("claim_type", "adjuster_team").orderBy("claimed_amount_distance_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type = Window.partitionBy("claim_type")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_claim_type_adjuster_team_by_2 = Window.partitionBy("claim_type", "adjuster_team").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_claims
    .filter(F.col("claim_type") != "delay")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.abs(F.col("claimed_amount") - 20710.1).alias("claimed_amount_distance"),
        F.abs(F.col("claimed_amount") - 28755.2).alias("claimed_amount_distance_2"),
    )
)

with_parent_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.team").isin("Motor", "Travel")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.reported_at",
        "s.claimed_amount_distance_2",
        F.col("a.team").alias("adjuster_team"),
    )
)

metrics = (
    with_parent_adjuster
    .select(
        "*",
        F.last("reported_at", ignorenulls=True).over(w_all_by_reported_at_running).alias("last_known_reported_at"),
        F.ntile(5).over(w_adjuster_team_claim_type_by).alias("approved_amount_quintile"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id)).alias("distinct_adjuster_id_in_policy_id"),
        F.first("claimed_amount_distance_2", ignorenulls=True).over(w_claim_type_adjuster_team_by).alias("first_known_claimed_amount_distance_2"),
        F.round(F.col("claimed_amount_distance_2") - F.avg("claimed_amount_distance_2").over(w_claim_type), 2).alias("claimed_amount_distance_2_vs_claim_type_avg"),
        F.row_number().over(w_all_by_reported_at).alias("seq_no"),
        F.count(F.lit(1)).over(w_claim_type_adjuster_team_by_2).alias("running_count"),
        F.round(F.col("claimed_amount_distance_2") - F.avg("claimed_amount_distance_2").over(w_policy_id), 2).alias("claimed_amount_distance_2_vs_policy_id_avg"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.min("claimed_amount_distance_2").over(w_policy_id).alias("policy_id_min_claimed_amount_distance_2"),
    )
)

rollup_base = (
    metrics
    .groupBy("approved_amount_quintile")
    .agg(
        F.min("approved_amount").alias("min_approved_amount"),
        F.max("reported_at").alias("last_reported_at"),
        F.sum("running_count").alias("sum_running_count_check"),
    )
    .filter(F.col("sum_running_count_check") > 4)
    .drop("sum_running_count_check")
)

result = (
    rollup_base
    .filter(F.col("min_approved_amount") < 35000.0)
    .select("approved_amount_quintile", "min_approved_amount", "last_reported_at")
)
