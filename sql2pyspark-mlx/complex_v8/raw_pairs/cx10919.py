from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_policy_holder_id = Window.partitionBy("claim_type", "policy_holder_id").orderBy("claim_date", "id")
w_all_by_greatest_approved_amo = Window.orderBy(F.col("greatest_approved_amount_claimed_amount").asc_nulls_last(), "id")
w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_claim_type_policy_holder_id_2 = Window.partitionBy("claim_type", "policy_holder_id").orderBy("claim_date")
w_all_by_approved_amount_full = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_greatest_approved_amo_2 = Window.orderBy(F.col("greatest_approved_amount_claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

with_parent_policy = (
    base_claims.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.status").isin("EXPIRED", "ACTIVE")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.greatest_approved_amount_claimed_amount",
        F.col("p.holder_id").alias("policy_holder_id"),
    )
)

with_windows = (
    with_parent_policy
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "policy_holder_id",
        F.lead("claim_date", 2).over(w_claim_type_policy_holder_id).alias("next2_claim_date"),
        F.round(F.percent_rank().over(w_all_by_greatest_approved_amo), 3).alias("greatest_approved_amount_claimed_amount_percent_"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_claim_type), 2).alias("approved_amount_vs_adjuster_id_claim_type_avg"),
        F.dense_rank().over(w_claim_type_policy_holder_id_2).alias("claim_date_dense_rank"),
        F.round(F.percent_rank().over(w_claim_type_policy_holder_id_2), 3).alias("claim_date_percent_rank"),
        F.nth_value("claim_date", 2).over(w_all_by_approved_amount_full).alias("second_claim_date"),
        F.row_number().over(w_claim_type_policy_holder_id).alias("rn"),
        F.lag("approved_amount").over(w_claim_type_policy_holder_id).alias("prev_approved_amount"),
        F.last("claim_date", ignorenulls=True).over(w_all_by_greatest_approved_amo_2).alias("last_known_claim_date"),
        F.min("approved_amount").over(w_adjuster_id_claim_type).alias("adjuster_id_claim_type_min_approved_amount"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_all_by_greatest_approved_amo)).alias("approved_amount_change"),
        F.ntile(5).over(w_claim_type_policy_holder_id).alias("claim_date_quintile"),
    )
)

leaders = (
    with_windows
    .filter(F.col("rn") <= 5)
    .drop("rn")
)

result = (
    leaders
    .filter(F.col("approved_amount_vs_adjuster_id_claim_type_avg") < 15000.0)
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "policy_holder_id",
        "next2_claim_date",
        "greatest_approved_amount_claimed_amount_percent_",
        "approved_amount_vs_adjuster_id_claim_type_avg",
        "claim_date_dense_rank",
        "claim_date_percent_rank",
        "prev_approved_amount",
        "last_known_claim_date",
        "adjuster_id_claim_type_min_approved_amount",
        "approved_amount_change",
        F.date_add(F.col("last_known_claim_date"), 365).alias("last_known_claim_date_plus_365d"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)
