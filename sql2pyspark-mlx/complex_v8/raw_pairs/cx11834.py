from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_adjuster_leve = Window.partitionBy("adjuster_id").orderBy(F.col("adjuster_level").asc_nulls_last(), "id")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_adjuster_id_status_by_adjust = Window.partitionBy("adjuster_id", "status").orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_adjuster_level_runnin = Window.orderBy(F.col("adjuster_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status = Window.partitionBy("status")
w_all_by_adjuster_level = Window.orderBy(F.col("adjuster_level").desc(), "id")
w_adjuster_id_policy_id_by_app = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_adjuster_level_ties = Window.orderBy(F.col("adjuster_level").desc())
w_all_by_running_approved_amou = Window.orderBy(F.col("running_approved_amount").asc_nulls_last())
w_approved_amount_tercile_poli = Window.partitionBy("approved_amount_tercile", "policy_id").orderBy("adjuster_level_dense_rank", "id")
w_second_adjuster_level = Window.partitionBy("second_adjuster_level")
w_all_by_prev_approved_amount = Window.orderBy(F.col("prev_approved_amount").desc(), "id")

recent = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

with_details = (
    recent.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.status",
        F.col("a.level").alias("adjuster_level"),
        "a.adjuster_name",
    )
)

ranked = (
    with_details
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "adjuster_level",
        F.row_number().over(w_adjuster_id_by_adjuster_leve).alias("rn"),
        F.count(F.lit(1)).over(w_status_adjuster_id).alias("rows_in_status_adjuster_id"),
        F.ntile(3).over(w_all_by_approved_amount).alias("approved_amount_tercile"),
        F.sum("approved_amount").over(w_adjuster_id_status_by_adjust).alias("running_approved_amount"),
        F.max("adjuster_level").over(w_all_by_adjuster_level_runnin).alias("max_adjuster_level_so_far"),
        F.round(F.avg("approved_amount").over(w_status), 2).alias("status_avg_approved_amount"),
        F.lag("approved_amount").over(w_all_by_adjuster_level).alias("prev_approved_amount"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_all_by_approved_amount)).alias("approved_amount_change"),
        F.nth_value("adjuster_level", 2).over(w_adjuster_id_policy_id_by_app).alias("second_adjuster_level"),
        F.dense_rank().over(w_all_by_adjuster_level_ties).alias("adjuster_level_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_approved_amount), 3).alias("approved_amount_percent_rank"),
    )
)

leaders = ranked.filter((F.col("rn") <= 5) & (F.col("approved_amount_tercile") >= 3))

ranked_2 = (
    leaders
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "adjuster_level",
        "approved_amount_tercile",
        "running_approved_amount",
        "max_adjuster_level_so_far",
        "status_avg_approved_amount",
        "approved_amount_change",
        "second_adjuster_level",
        "adjuster_level_dense_rank",
        F.round(F.percent_rank().over(w_all_by_running_approved_amou), 3).alias("running_approved_amount_percent_rank"),
        (F.col("adjuster_level") - F.lag("adjuster_level").over(w_approved_amount_tercile_poli)).alias("adjuster_level_change"),
        F.round(F.col("adjuster_level") - F.avg("adjuster_level").over(w_second_adjuster_level), 2).alias("adjuster_level_vs_second_adjuster_level_avg"),
        F.lag("rows_in_status_adjuster_id").over(w_all_by_prev_approved_amount).alias("prev_rows_in_status_adjuster_id"),
    )
)

result = (
    ranked_2
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "status",
        "adjuster_level",
        "approved_amount_tercile",
        "running_approved_amount",
        "max_adjuster_level_so_far",
        "status_avg_approved_amount",
        "approved_amount_change",
        "second_adjuster_level",
        "adjuster_level_dense_rank",
        "adjuster_level_change",
        "adjuster_level_vs_second_adjuster_level_avg",
        "prev_rows_in_status_adjuster_id",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)
