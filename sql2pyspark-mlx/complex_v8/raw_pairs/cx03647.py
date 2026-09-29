from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_status_by_reported_at_ties = Window.partitionBy("status").orderBy("reported_at")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_reported_at_dense_rank_claim = Window.partitionBy("reported_at_dense_rank", "claim_type").orderBy(F.col("claimed_amount_share_of_adjuster_id_policy_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_dense_rank_repor = Window.partitionBy("reported_at_dense_rank", F.to_date("reported_at")).orderBy(F.col("claimed_amount").desc())
w_reported_at_dense_rank_repor_2 = Window.partitionBy("reported_at_dense_rank", F.to_date("reported_at")).orderBy(F.col("claimed_amount").desc(), "id")

scoped = (
    df_claims
    .filter((F.col("claimed_amount") > 19337.6) | (F.col("approved_amount") <= 15857.9))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_windows = (
    scoped
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id_policy_id"),
        F.count(F.lit(1)).over(w_all_by_claimed_amount_runnin).alias("running_count"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_policy_id), 2).alias("adjuster_id_policy_id_avg_claimed_amount"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.dense_rank().over(w_status_by_reported_at_ties).alias("reported_at_dense_rank"),
        F.lag("claimed_amount", 3).over(w_all_by_claimed_amount).alias("prev3_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_adjuster_id_policy_id), 2).alias("claimed_amount_vs_adjuster_id_policy_id_avg"),
        F.round(F.avg("approved_amount").over(w_policy_id), 2).alias("policy_id_avg_approved_amount"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("reported_at_dense_rank") <= 3) & (F.col("policy_id").isNotNull()))
)

windowed = (
    first_rows
    .select(
        "*",
        F.min("reported_at_dense_rank").over(w_reported_at_dense_rank_claim).alias("min_reported_at_dense_rank_so_far"),
        F.rank().over(w_reported_at_dense_rank_repor).alias("claimed_amount_rank"),
        F.round(F.percent_rank().over(w_reported_at_dense_rank_repor_2), 3).alias("claimed_amount_percent_rank"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "claimed_amount_share_of_adjuster_id_policy_id",
        "running_count",
        "adjuster_id_policy_id_avg_claimed_amount",
        "rows_in_policy_id",
        "reported_at_dense_rank",
        "prev3_claimed_amount",
        "claimed_amount_vs_adjuster_id_policy_id_avg",
        "claimed_amount_percent_rank",
        "rows_in_policy_id_2",
    )
    .orderBy("reported_at_dense_rank")
)
