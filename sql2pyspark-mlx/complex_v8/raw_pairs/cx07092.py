from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_claimed_amount = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount_plus_approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_approved_amount = Window.partitionBy("status").orderBy("approved_amount", "id")
w_policy_id_by_claim_date_runn = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_status = Window.partitionBy("status")
w_all_by_claim_date_full = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_status_by_approved_amount_ti = Window.partitionBy("status").orderBy("approved_amount")
w_policy_id_by_claimed_amount_2 = Window.partitionBy("policy_id").orderBy(F.col("claimed_amount_plus_approved_amount").desc())
w_policy_id_by_claim_date_ties = Window.partitionBy("policy_id").orderBy("claim_date")
w_policy_id_by_claim_date_prio = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(-1, -1)

base_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(10, 13)) & (F.col("claim_date") < F.lit("2024-08-20").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

with_trends = (
    base_claims
    .select(
        "*",
        F.last("approved_amount", ignorenulls=True).over(w_policy_id_by_claimed_amount).alias("last_known_approved_amount"),
        F.row_number().over(w_status_by_approved_amount).alias("rn"),
        F.last("reported_at", ignorenulls=True).over(w_policy_id_by_claim_date_runn).alias("last_known_reported_at"),
        F.count(F.lit(1)).over(w_status_adjuster_id).alias("rows_in_status_adjuster_id"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_status_adjuster_id"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
        F.nth_value("claim_date", 2).over(w_all_by_claim_date_full).alias("second_claim_date"),
        F.sum("approved_amount").over(w_status_adjuster_id).alias("status_adjuster_id_sum_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status_adjuster_id), 2).alias("approved_amount_vs_status_adjuster_id_avg"),
        F.lag("claim_date", 2).over(w_all_by_claim_date).alias("prev2_claim_date"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_status), 2).alias("approved_amount_vs_status_avg"),
        F.dense_rank().over(w_status_by_approved_amount_ti).alias("approved_amount_dense_rank"),
        F.dense_rank().over(w_policy_id_by_claimed_amount_2).alias("claimed_amount_plus_approved_amount_dense_rank"),
        F.rank().over(w_policy_id_by_claim_date_ties).alias("claim_date_rank"),
        F.round(F.avg("approved_amount").over(w_policy_id_by_claim_date_prio), 2).alias("approved_amount_avg_1_prior"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "claim_type_ref",
        "last_known_approved_amount",
        "last_known_reported_at",
        "approved_amount_share_of_status_adjuster_id",
        "distinct_adjuster_id_in_status",
        "approved_amount_vs_status_adjuster_id_avg",
        "prev2_claim_date",
        "approved_amount_vs_status_avg",
        "claimed_amount_plus_approved_amount_dense_rank",
        "claim_date_rank",
        "approved_amount_avg_1_prior",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
    .orderBy(
        F.col("approved_amount_vs_status_avg").asc(),
        F.col("last_known_reported_at").desc(),
    )
)
