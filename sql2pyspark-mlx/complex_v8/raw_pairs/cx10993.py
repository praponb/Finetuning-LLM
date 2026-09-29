from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_reported_at_tie = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc())
w_policy_id_by_reported_at_mov = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_adjuster_id_by_reported_at_m = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_by_reported_at_mov_2 = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_adjuster_id_by_reported_at = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc(), "id")
w_adjuster_id_by_reported_at_r = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_approved_amount = Window.partitionBy("status").orderBy(F.col("approved_amount").asc_nulls_last(), "id")

recent = df_claims.filter(F.col("policy_id") <= 22)

with_windows = (
    recent
    .select(
        "*",
        F.round(F.percent_rank().over(w_policy_id_by_reported_at_tie), 3).alias("reported_at_percent_rank"),
        F.round(F.avg("approved_amount").over(w_policy_id_by_reported_at_mov), 2).alias("approved_amount_avg_3"),
        F.max("claimed_amount").over(w_adjuster_id_by_reported_at_m).alias("claimed_amount_max_3"),
        F.lag("reported_at").over(w_all_by_approved_amount).alias("prev_reported_at"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.lag("approved_amount").over(w_all_by_approved_amount).alias("prev_approved_amount"),
        F.lag("approved_amount", 1, 0).over(w_all_by_approved_amount).alias("prev_approved_amount_2"),
        F.min("approved_amount").over(w_policy_id).alias("policy_id_min_approved_amount"),
        F.round(F.avg("approved_amount").over(w_policy_id_by_reported_at_mov_2), 2).alias("approved_amount_avg_4"),
        F.lag("claimed_amount", 3, 0).over(w_adjuster_id_by_reported_at).alias("prev3_claimed_amount"),
        F.sum("claimed_amount").over(w_adjuster_id_by_reported_at_r).alias("running_claimed_amount"),
        F.ntile(3).over(w_status_by_approved_amount).alias("approved_amount_tercile"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "reported_at_percent_rank",
        "approved_amount_avg_3",
        "claimed_amount_max_3",
        "rows_in_policy_id",
        "prev_approved_amount",
        "prev_approved_amount_2",
        "approved_amount_avg_4",
        "prev3_claimed_amount",
        "running_claimed_amount",
        (F.col("reported_at") + F.expr("INTERVAL 2 HOURS")).alias("reported_at_plus_2h"),
    )
    .orderBy(F.col("claimed_amount").asc_nulls_last())
)
