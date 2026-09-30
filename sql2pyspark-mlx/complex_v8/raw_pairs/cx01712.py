from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_reported_at_ful = Window.partitionBy("policy_id").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_reported_at_pri = Window.partitionBy("policy_id").orderBy("reported_at", "id").rowsBetween(-3, -1)
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy("approved_amount", "id")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_ties = Window.orderBy("reported_at")
w_all_by_approved_amount_prior = Window.orderBy("approved_amount", "id").rowsBetween(-1, -1)
w_policy_id_adjuster_id = Window.partitionBy("policy_id", "adjuster_id")
w_all_by_approved_amount_full = Window.orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_claims = (
    df_claims
    .filter(F.col("claim_date") < F.lit("2025-04-25").cast("date"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
    )
)

with_trends = (
    base_claims
    .select(
        "*",
        F.nth_value("approved_amount", 2).over(w_policy_id_by_reported_at_ful).alias("second_approved_amount"),
        F.sum("approved_amount").over(w_policy_id_by_reported_at_pri).alias("approved_amount_sum_3_prior"),
        F.last("reported_at").over(w_claim_type_by_approved_amoun).alias("last_reported_at"),
        F.round(F.cume_dist().over(w_all_by_approved_amount), 3).alias("approved_amount_cume_dist"),
        F.min("approved_amount").over(w_all_by_reported_at_running).alias("min_approved_amount_so_far"),
        F.round(F.cume_dist().over(w_all_by_reported_at_ties), 3).alias("reported_at_cume_dist"),
        F.sum("approved_amount").over(w_all_by_approved_amount_prior).alias("approved_amount_sum_1_prior"),
        F.min("approved_amount").over(w_policy_id_adjuster_id).alias("policy_id_adjuster_id_min_approved_amount"),
        F.size(F.collect_set("claim_type").over(w_policy_id_adjuster_id)).alias("distinct_claim_type_in_policy_id_adjuster_id"),
        F.last("reported_at").over(w_all_by_approved_amount_full).alias("last_reported_at_2"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_adjuster_id"),
    )
)

enriched = (
    with_trends
    .filter(F.col("policy_id_adjuster_id_min_approved_amount").between(125000.0, 450000.0))
    .select(
        "*",
        F.round(F.col("second_approved_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("second_approved_amount_per_approved_amount"),
        F.date_trunc("year", F.col("last_reported_at")).alias("last_reported_at_year"),
    )
)

result = (
    enriched
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "approved_amount_sum_3_prior",
        "last_reported_at",
        "approved_amount_cume_dist",
        "min_approved_amount_so_far",
        "approved_amount_sum_1_prior",
        "policy_id_adjuster_id_min_approved_amount",
        "distinct_claim_type_in_policy_id_adjuster_id",
        "second_approved_amount_per_approved_amount",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_reported_at_2")).alias("days_since_last_reported_at_2"),
        F.year(F.col("reported_at")).alias("reported_at_year"),
    )
    .orderBy(F.col("approved_amount_cume_dist").desc())
)
