from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_reported_at_to_date_b = Window.partitionBy("status", F.to_date("reported_at")).orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_status_reported_at_to_date_b_2 = Window.partitionBy("status", F.to_date("reported_at")).orderBy("claim_date", "id")
w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").desc())
w_distinct_policy_id_in_adjust = Window.partitionBy("distinct_policy_id_in_adjuster_id", F.month("reported_at"))
w_status_by_adjuster_id_sum_cl = Window.partitionBy("status").orderBy(F.col("adjuster_id_sum_claimed_amount").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_status_by_adjuster_id_sum_cl_2 = Window.partitionBy("status").orderBy(F.col("adjuster_id_sum_claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_adjuster_id_sum_cl_3 = Window.partitionBy("status").orderBy(F.col("adjuster_id_sum_claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_adjuster_id_sum_cl_4 = Window.partitionBy("status").orderBy(F.col("adjuster_id_sum_claimed_amount").desc_nulls_first(), "id").rowsBetween(-3, -1)
w_status_by_adjuster_id_sum_cl_5 = Window.partitionBy("status").orderBy(F.col("adjuster_id_sum_claimed_amount").desc_nulls_first(), "id")

base_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 5) & (F.col("approved_amount") <= 14377.6))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

joined = (
    base_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        F.col("a.level").alias("adjuster_level"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.nth_value("adjuster_level", 2).over(w_status_reported_at_to_date_b).alias("second_adjuster_level"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.sum("claimed_amount").over(w_adjuster_id).alias("adjuster_id_sum_claimed_amount"),
        F.round(F.avg("adjuster_level").over(w_adjuster_id), 2).alias("adjuster_id_avg_adjuster_level"),
        F.nth_value("reported_at", 2).over(w_status_reported_at_to_date_b).alias("second_reported_at"),
        F.rank().over(w_status_reported_at_to_date_b_2).alias("claim_date_rank"),
        F.round(F.cume_dist().over(w_all_by_claimed_amount_ties), 3).alias("claimed_amount_cume_dist"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
    )
)

top_per_group = (
    ranked
    .filter(F.col("claim_date_rank") <= 2)
    .drop("claim_date_rank")
)

with_windows = (
    top_per_group
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "reported_at",
        "adjuster_level",
        "second_adjuster_level",
        "adjuster_id_sum_claimed_amount",
        "adjuster_id_avg_adjuster_level",
        "second_reported_at",
        F.count(F.lit(1)).over(w_distinct_policy_id_in_adjust).alias("rows_in_distinct_policy_id_in_adjuster_id_report"),
        F.round(F.avg("rows_in_adjuster_id").over(w_status_by_adjuster_id_sum_cl), 2).alias("rows_in_adjuster_id_avg_2"),
        F.last("second_reported_at", ignorenulls=True).over(w_status_by_adjuster_id_sum_cl_2).alias("last_known_second_reported_at"),
        F.first("rows_in_adjuster_id").over(w_status_by_adjuster_id_sum_cl_3).alias("first_rows_in_adjuster_id"),
        F.round(F.avg("rows_in_adjuster_id").over(w_status_by_adjuster_id_sum_cl_4), 2).alias("rows_in_adjuster_id_avg_3_prior"),
        F.row_number().over(w_status_by_adjuster_id_sum_cl_5).alias("rn"),
        F.lead("claimed_amount_cume_dist").over(w_status_by_adjuster_id_sum_cl_5).alias("next_claimed_amount_cume_dist"),
    )
)

result = (
    with_windows
    .filter(F.col("rows_in_adjuster_id_avg_3_prior") >= 7.7)
    .select("next_claimed_amount_cume_dist", "first_rows_in_adjuster_id", "rn")
    .distinct()
)
