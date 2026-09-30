from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_adjuster_level_status_by_adj = Window.partitionBy("adjuster_level", "status").orderBy(F.col("adjuster_level").desc_nulls_first(), "id")
w_adjuster_level_by_adjuster_l = Window.partitionBy("adjuster_level").orderBy("adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount_runni = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_approved_amount_full = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount_movin = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-2, Window.currentRow)
w_adjuster_level_by_adjuster_l_2 = Window.partitionBy("adjuster_level").orderBy("adjuster_level", "id").rowsBetween(-1, -1)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_level_status_by_adj_2 = Window.partitionBy("adjuster_level", "status").orderBy(F.col("adjuster_level").desc_nulls_first())
w_adjuster_level_by_adjuster_l_3 = Window.partitionBy("adjuster_level").orderBy("adjuster_level")

prepared = (
    df_claims
    .filter(F.col("status").isin("IN_REVIEW", "REJECTED", "APPROVED"))
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

combined_details = (
    prepared.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
        F.col("a.level").alias("adjuster_level"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_adjuster_level_status_by_adj)).alias("adjuster_level_change"),
        F.nth_value("adjuster_level", 2).over(w_adjuster_level_by_adjuster_l).alias("second_adjuster_level"),
        F.sum("approved_amount").over(w_status).alias("status_sum_approved_amount"),
        F.last("adjuster_level", ignorenulls=True).over(w_all_by_approved_amount_runni).alias("last_known_adjuster_level"),
        F.nth_value("approved_amount", 2).over(w_all_by_approved_amount_full).alias("second_approved_amount"),
        F.sum("approved_amount").over(w_all_by_approved_amount_movin).alias("approved_amount_sum_3"),
        F.round(F.avg("approved_amount").over(w_adjuster_level_by_adjuster_l_2), 2).alias("approved_amount_avg_1_prior"),
        F.first("approved_amount").over(w_adjuster_level_by_adjuster_l).alias("first_approved_amount"),
        F.row_number().over(w_all_by_approved_amount).alias("rn"),
        F.round(F.percent_rank().over(w_adjuster_level_status_by_adj_2), 3).alias("adjuster_level_percent_rank"),
        F.round(F.col("adjuster_level") / F.nullif(F.sum("adjuster_level").over(w_status), F.lit(0)), 4).alias("adjuster_level_share_of_status"),
        F.dense_rank().over(w_adjuster_level_by_adjuster_l_3).alias("adjuster_level_dense_rank"),
        F.rank().over(w_all_by_approved_amount).alias("approved_amount_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "adjuster_name",
        "adjuster_level",
        "rows_in_status",
        "second_adjuster_level",
        "status_sum_approved_amount",
        "last_known_adjuster_level",
        "approved_amount_sum_3",
        "approved_amount_avg_1_prior",
        "first_approved_amount",
        "adjuster_level_share_of_status",
        "approved_amount_rank",
        F.round(F.col("second_approved_amount") / F.nullif(F.col("approved_amount_rank"), F.lit(0)), 3).alias("second_approved_amount_per_approved_amount_rank"),
    )
    .orderBy(F.col("approved_amount_avg_1_prior").desc_nulls_first())
)
