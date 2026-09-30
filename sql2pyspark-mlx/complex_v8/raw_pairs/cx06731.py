from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount_ties = Window.orderBy(F.col("claimed_amount").asc_nulls_last())
w_status_claim_type = Window.partitionBy("status", "claim_type")
w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").asc_nulls_last(), "id")

filtered = (
    df_claims
    .filter((F.col("claim_type") != "flood") | (F.col("status").isNotNull()))
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
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_dense_rank"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_status_claim_type"),
        F.first("reported_at").over(w_all_by_reported_at_full).alias("first_reported_at"),
        F.first("approved_amount", ignorenulls=True).over(w_all_by_reported_at_running).alias("first_known_approved_amount"),
        F.row_number().over(w_all_by_claimed_amount).alias("seq_no"),
    )
)

result = (
    with_windows
    .rollup("adjuster_id", "policy_id")
    .agg(
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("claimed_amount_dense_rank"), F.lit(0)), 3).alias("claimed_amount_to_claimed_amount_dense_rank_rati"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.max("claim_date").alias("last_claim_date"),
    )
    .orderBy(
        F.col("adjuster_id").asc_nulls_last(),
        F.col("policy_id").asc_nulls_last(),
    )
)
