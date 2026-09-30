from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_clai = Window.partitionBy("policy_id", "claim_type").orderBy("claimed_amount", "id").rowsBetween(-6, -1)
w_status_policy_id_by_claimed = Window.partitionBy("status", "policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type = Window.partitionBy("claim_type")
w_adjuster_id_status_by_approv = Window.partitionBy("adjuster_id", "status").orderBy("approved_amount")
w_status_policy_id_by_claimed_2 = Window.partitionBy("status", "policy_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_status_by_approv_2 = Window.partitionBy("adjuster_id", "status").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_claim_type_by_clai_2 = Window.partitionBy("policy_id", "claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")

scoped = (
    df_claims
    .filter(F.col("claim_type") != "fire")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.round(F.avg("approved_amount").over(w_policy_id_claim_type_by_clai), 2).alias("approved_amount_avg_6_prior"),
        F.first("claimed_amount").over(w_status_policy_id_by_claimed).alias("first_claimed_amount"),
        F.max("claimed_amount").over(w_claim_type).alias("claim_type_max_claimed_amount"),
        F.round(F.percent_rank().over(w_adjuster_id_status_by_approv), 3).alias("approved_amount_percent_rank"),
        F.first("approved_amount", ignorenulls=True).over(w_status_policy_id_by_claimed_2).alias("first_known_approved_amount"),
        F.nth_value("approved_amount", 2).over(w_adjuster_id_status_by_approv_2).alias("second_approved_amount"),
        F.last("claimed_amount", ignorenulls=True).over(w_policy_id_claim_type_by_clai_2).alias("last_known_claimed_amount"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id"),
        F.size(F.collect_set("adjuster_id").over(w_claim_type)).alias("distinct_adjuster_id_in_claim_type"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "approved_amount_avg_6_prior",
        "first_claimed_amount",
        "claim_type_max_claimed_amount",
        "first_known_approved_amount",
        "last_known_claimed_amount",
        "claimed_amount_share_of_adjuster_id",
        "distinct_adjuster_id_in_claim_type",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)
