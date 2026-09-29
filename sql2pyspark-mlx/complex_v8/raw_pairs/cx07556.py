from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_policy_id_by_claim_date_runn = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_all_by_claim_date_full = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_running = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_moving_3 = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_approved_amount_full = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_claims = (
    df_claims
    .filter(F.col("approved_amount") > 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
    )
)

with_windows = (
    base_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        F.round(F.cume_dist().over(w_all_by_claim_date), 3).alias("claim_date_cume_dist"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank"),
        F.rank().over(w_all_by_claim_date).alias("claim_date_rank_2"),
        F.lead("approved_amount", 3).over(w_policy_id_by_claim_date).alias("next3_approved_amount"),
        F.count(F.lit(1)).over(w_policy_id_by_claim_date_runn).alias("running_count"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id"),
        F.lag("claim_date", 2).over(w_all_by_approved_amount).alias("prev2_claim_date"),
        F.row_number().over(w_all_by_approved_amount).alias("rn"),
        F.first("claim_date").over(w_all_by_claim_date_full).alias("first_claim_date"),
        F.last("approved_amount", ignorenulls=True).over(w_all_by_claim_date_running).alias("last_known_approved_amount"),
        F.round(F.avg("approved_amount").over(w_all_by_claim_date_moving_3), 2).alias("approved_amount_avg_4"),
        F.last("claim_date").over(w_all_by_approved_amount_full).alias("last_claim_date"),
        F.nth_value("approved_amount", 2).over(w_all_by_approved_amount_full).alias("second_approved_amount"),
    )
)

top_per_group = with_windows.filter(F.col("claim_date_rank") <= 5)

result = (
    top_per_group
    .filter(F.col("policy_id") >= 16)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "claim_date_cume_dist",
        "claim_date_rank",
        "claim_date_rank_2",
        "approved_amount_share_of_policy_id",
        "prev2_claim_date",
        "first_claim_date",
        "approved_amount_avg_4",
        "last_claim_date",
        "second_approved_amount",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
    .orderBy("last_claim_date", "id")
    .limit(20)
)
