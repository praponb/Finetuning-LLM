from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date_running = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount_ties = Window.orderBy("claimed_amount")
w_reported_at_to_date_adjuster = Window.partitionBy(F.to_date("reported_at"), "adjuster_id").orderBy(F.col("claimed_amount_bucket").desc(), "id")
w_all_by_claim_date_full = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_policy_id_status = Window.partitionBy("policy_id", "status")

scoped_claims = (
    df_claims
    .filter((F.col("status") == "REJECTED") & (F.col("claimed_amount") <= 42447.2))
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
        F.least(F.col("claimed_amount"), F.col("approved_amount")).alias("least_claimed_amount_approved_amount"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

windowed = (
    scoped_claims
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "least_claimed_amount_approved_amount",
        F.last("claimed_amount_bucket", ignorenulls=True).over(w_all_by_claim_date_running).alias("last_known_claimed_amount_bucket"),
        F.dense_rank().over(w_all_by_claimed_amount_ties).alias("claimed_amount_dense_rank"),
        F.round(F.cume_dist().over(w_reported_at_to_date_adjuster), 3).alias("claimed_amount_bucket_cume_dist"),
        F.last("reported_at").over(w_all_by_claim_date_full).alias("last_reported_at"),
        F.sum("claimed_amount").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_sum_claimed_amount"),
        F.min("claimed_amount").over(w_policy_id).alias("policy_id_min_claimed_amount"),
        F.row_number().over(w_all_by_approved_amount).alias("rn"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_reported_at_to_date_adjuster)).alias("claimed_amount_change"),
        F.min("approved_amount").over(w_policy_id_status).alias("policy_id_status_min_approved_amount"),
    )
)

result = (
    windowed
    .select(
        "id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "least_claimed_amount_approved_amount",
        "last_known_claimed_amount_bucket",
        "claimed_amount_dense_rank",
        "claimed_amount_bucket_cume_dist",
        "last_reported_at",
        "adjuster_id_policy_id_sum_claimed_amount",
        "rn",
        "policy_id_status_min_approved_amount",
    )
    .orderBy(F.col("rn").desc(), F.col("claimed_amount_dense_rank").desc())
)
