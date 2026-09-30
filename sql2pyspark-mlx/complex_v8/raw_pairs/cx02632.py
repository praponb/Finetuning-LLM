from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_type = Window.partitionBy("status", "claim_type")
w_reported_at_year_by_reported = Window.partitionBy(F.year("reported_at")).orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_policy_id = Window.partitionBy("status", "policy_id")
w_status_by_approved_amount = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id")
w_policy_id_adjuster_id_by_rep = Window.partitionBy("policy_id", "adjuster_id").orderBy(F.col("reported_at").desc(), "id")
w_all_by_reported_at_moving_2 = Window.orderBy("reported_at", "id").rowsBetween(-2, Window.currentRow)
w_status_by_approved_amount_fu = Window.partitionBy("status").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_year_by_reported_2 = Window.partitionBy(F.year("reported_at")).orderBy("reported_at", "id")
w_policy_id_by_reported_at_run = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type = Window.partitionBy("claim_type")
w_policy_id_by_reported_at_tie = Window.partitionBy("policy_id").orderBy(F.col("reported_at").desc())
w_status_by_claimed_amount_sha = Window.partitionBy("status").orderBy("claimed_amount_share_of_status_policy_id", "id")

base_claims = (
    df_claims
    .filter((F.col("claim_type").isNotNull()) & (F.col("policy_id").isNotNull()))
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
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
    )
)

with_windows = (
    base_claims
    .select(
        "*",
        F.min("approved_amount").over(w_status_claim_type).alias("status_claim_type_min_approved_amount"),
        F.min("approved_amount").over(w_reported_at_year_by_reported).alias("min_approved_amount_so_far"),
        F.min("claimed_amount").over(w_status_policy_id).alias("status_policy_id_min_claimed_amount"),
        F.lead("claim_date").over(w_status_by_approved_amount).alias("next_claim_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_status_policy_id"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_claim_type), 2).alias("claimed_amount_vs_status_claim_type_avg"),
        F.lag("claim_date").over(w_policy_id_adjuster_id_by_rep).alias("prev_claim_date"),
        F.max("approved_amount").over(w_all_by_reported_at_moving_2).alias("approved_amount_max_3"),
        F.first("claimed_amount").over(w_status_by_approved_amount_fu).alias("first_claimed_amount"),
        F.size(F.collect_set("adjuster_id").over(w_status_policy_id)).alias("distinct_adjuster_id_in_status_policy_id"),
        F.lead("reported_at", 3).over(w_reported_at_year_by_reported_2).alias("next3_reported_at"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "status_claim_type_min_approved_amount",
        "status_policy_id_min_claimed_amount",
        "claimed_amount_share_of_status_policy_id",
        "claimed_amount_vs_status_claim_type_avg",
        "approved_amount_max_3",
        "distinct_adjuster_id_in_status_policy_id",
        F.last("first_claimed_amount", ignorenulls=True).over(w_policy_id_by_reported_at_run).alias("last_known_first_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type), 2).alias("claimed_amount_vs_claim_type_avg"),
        F.dense_rank().over(w_policy_id_by_reported_at_tie).alias("reported_at_dense_rank"),
        F.lag("min_approved_amount_so_far").over(w_status_by_claimed_amount_sha).alias("prev_min_approved_amount_so_far"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "policy_id",
        "status_claim_type_min_approved_amount",
        "status_policy_id_min_claimed_amount",
        "claimed_amount_share_of_status_policy_id",
        "claimed_amount_vs_status_claim_type_avg",
        "approved_amount_max_3",
        "distinct_adjuster_id_in_status_policy_id",
        "claimed_amount_vs_claim_type_avg",
        "reported_at_dense_rank",
        "prev_min_approved_amount_so_far",
        "rows_in_claim_type",
        F.substring(F.trim(F.col("status")), 1, 2).alias("status_prefix"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
