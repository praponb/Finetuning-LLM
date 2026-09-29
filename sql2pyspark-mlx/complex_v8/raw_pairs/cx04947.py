from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount").desc(), "id")
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claimed_amount").desc(), "id")
w_status_policy_id = Window.partitionBy("status", "policy_id")
w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_claim_type_adjuster_id_by_ap = Window.partitionBy("claim_type", "adjuster_id").orderBy("approved_amount_per_claimed_amount", "id")
w_status_adjuster_id_by_approv = Window.partitionBy("status", "adjuster_id").orderBy("approved_amount_per_claimed_amount")
w_status_adjuster_id_by_approv_2 = Window.partitionBy("status", "adjuster_id").orderBy("approved_amount_per_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_claims = (
    df_claims
    .filter(F.col("claimed_amount") > 4868.7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        F.round(F.col("approved_amount") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("approved_amount_per_claimed_amount"),
    )
)

sequenced = (
    prepared_claims
    .select(
        "*",
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_by_claimed_amount)).alias("claimed_amount_change"),
        F.lag("claimed_amount", 2).over(w_claim_type_adjuster_id_by_cl).alias("prev2_claimed_amount"),
        F.size(F.collect_set("claim_type").over(w_status_policy_id)).alias("distinct_claim_type_in_status_policy_id"),
        F.min("claimed_amount").over(w_claim_type_adjuster_id).alias("claim_type_adjuster_id_min_claimed_amount"),
        F.rank().over(w_claim_type_adjuster_id_by_cl).alias("claimed_amount_rank"),
        F.count(F.lit(1)).over(w_claim_type_adjuster_id).alias("rows_in_claim_type_adjuster_id"),
        F.dense_rank().over(w_claim_type_adjuster_id_by_ap).alias("approved_amount_per_claimed_amount_dense_rank"),
        F.round(F.cume_dist().over(w_status_adjuster_id_by_approv), 3).alias("approved_amount_per_claimed_amount_cume_dist"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_type_adjuster_id), F.lit(0)), 4).alias("claimed_amount_share_of_claim_type_adjuster_id"),
        F.last("claimed_amount").over(w_status_adjuster_id_by_approv_2).alias("last_claimed_amount"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "approved_amount_per_claimed_amount",
        "prev2_claimed_amount",
        "claimed_amount_rank",
        "rows_in_claim_type_adjuster_id",
        "approved_amount_per_claimed_amount_dense_rank",
        "approved_amount_per_claimed_amount_cume_dist",
        "claimed_amount_share_of_claim_type_adjuster_id",
        (F.col("claimed_amount") + F.col("last_claimed_amount")).alias("claimed_amount_plus_last_claimed_amount"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
    )
    .orderBy(F.col("id").desc(), F.col("adjuster_id").asc_nulls_last())
)
