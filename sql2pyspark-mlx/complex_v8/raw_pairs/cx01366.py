from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_by_claim_dat = Window.partitionBy(F.year("claim_date")).orderBy(F.col("claim_date").desc())
w_status_policy_id = Window.partitionBy("status", "policy_id")
w_status_by_least_approved_amo = Window.partitionBy("status").orderBy(F.col("least_approved_amount_claimed_amount").desc(), "id")
w_all_by_claim_date_prior_3 = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(-3, -1)
w_status_by_reported_at = Window.partitionBy("status").orderBy("reported_at", "id")
w_policy_id_by_least_approved = Window.partitionBy("policy_id").orderBy("least_approved_amount_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_least_approved_amo_2 = Window.partitionBy("status").orderBy(F.col("least_approved_amount_claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_reported_at_prior = Window.partitionBy("status").orderBy("reported_at", "id").rowsBetween(-1, -1)
w_next_reported_at_year_policy = Window.partitionBy(F.year("next_reported_at"), "policy_id").orderBy("claim_date", "id")
w_reported_at_quintile_claim_d = Window.partitionBy("reported_at_quintile", "claim_date_dense_rank")
w_next_reported_at_year_policy_2 = Window.partitionBy(F.year("next_reported_at"), "policy_id").orderBy("claim_date")

recent = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("approved_amount") <= 33690.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws("-", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.dense_rank().over(w_claim_date_year_by_claim_dat).alias("claim_date_dense_rank"),
        F.max("approved_amount").over(w_status_policy_id).alias("status_policy_id_max_approved_amount"),
        F.lead("reported_at").over(w_status_by_least_approved_amo).alias("next_reported_at"),
        F.round(F.avg("approved_amount").over(w_all_by_claim_date_prior_3), 2).alias("approved_amount_avg_3_prior"),
        F.ntile(5).over(w_status_by_reported_at).alias("reported_at_quintile"),
        F.nth_value("reported_at", 2).over(w_policy_id_by_least_approved).alias("second_reported_at"),
        F.max("claimed_amount").over(w_all_by_claim_date_prior_3).alias("claimed_amount_max_3_prior"),
        F.last("reported_at", ignorenulls=True).over(w_status_by_least_approved_amo_2).alias("last_known_reported_at"),
        F.size(F.collect_set("adjuster_id").over(w_status_policy_id)).alias("distinct_adjuster_id_in_status_policy_id"),
        F.round(F.avg("claimed_amount").over(w_status_by_reported_at_prior), 2).alias("claimed_amount_avg_1_prior"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "claim_type_status_label",
        "claim_date_dense_rank",
        "status_policy_id_max_approved_amount",
        "claimed_amount_max_3_prior",
        "last_known_reported_at",
        "claimed_amount_avg_1_prior",
        F.lag("claimed_amount_avg_1_prior", 3).over(w_next_reported_at_year_policy).alias("prev3_claimed_amount_avg_1_prior"),
        F.max("approved_amount").over(w_reported_at_quintile_claim_d).alias("reported_at_quintile_claim_date_dense_rank_max_a"),
        F.rank().over(w_next_reported_at_year_policy_2).alias("claim_date_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("claim_type_status_label").isNotNull())
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "claim_type_status_label",
        "claim_date_dense_rank",
        "status_policy_id_max_approved_amount",
        "claimed_amount_max_3_prior",
        "last_known_reported_at",
        "claimed_amount_avg_1_prior",
        "reported_at_quintile_claim_date_dense_rank_max_a",
        "claim_date_rank",
    )
    .orderBy(F.col("claim_date_dense_rank").desc(), "claim_date_rank")
)
