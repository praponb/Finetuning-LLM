from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_claimed_amount_tier_adjuster = Window.partitionBy("claimed_amount_tier", "adjuster_id").orderBy(F.col("claim_date").desc(), "id")
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_status_claim_date_year = Window.partitionBy("status", F.year("claim_date"))
w_all_by_claim_date_full = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_all_by_claim_date_moving_2 = Window.orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_first_claim_date_month_polic = Window.partitionBy(F.date_trunc("month", "first_claim_date"), "policy_id").orderBy(F.col("distinct_claim_type_in_status_claim_date_year").desc())
w_first_claim_date_month_polic_2 = Window.partitionBy(F.date_trunc("month", "first_claim_date"), "policy_id").orderBy(F.col("distinct_claim_type_in_status_claim_date_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_end_date_year_approve = Window.partitionBy(F.year("policy_end_date"), "approved_amount_dense_rank")
w_approved_amount_dense_rank_s = Window.partitionBy("approved_amount_dense_rank", "status").orderBy(F.col("claim_date_month_max_approved_amount").desc(), "id")
w_first_claim_date_month_polic_3 = Window.partitionBy(F.date_trunc("month", "first_claim_date"), "policy_id").orderBy(F.col("distinct_claim_type_in_status_claim_date_year").desc(), "id")
w_first_claim_date_month_polic_4 = Window.partitionBy(F.date_trunc("month", "first_claim_date"), "policy_id").orderBy(F.col("distinct_claim_type_in_status_claim_date_year").desc(), "id").rowsBetween(-3, Window.currentRow)

valid_claims = (
    df_claims
    .filter((F.col("status") == "PAID") & (F.col("claimed_amount") >= 4698.6))
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
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
        F.when(F.col("claimed_amount") >= 33975.8, "large").when(F.col("claimed_amount") >= 14019.8, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

enriched_policy = (
    valid_claims.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.approved_amount",
        "s.status",
        "s.claim_type_prefix",
        "s.claimed_amount_tier",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

with_trends = (
    enriched_policy
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "claim_type_prefix",
        "policy_end_date",
        F.max("approved_amount").over(w_claim_date_month).alias("claim_date_month_max_approved_amount"),
        F.lead("approved_amount", 1, 0).over(w_claimed_amount_tier_adjuster).alias("next_approved_amount"),
        F.dense_rank().over(w_all_by_approved_amount_ties).alias("approved_amount_dense_rank"),
        F.size(F.collect_set("claim_type").over(w_status_claim_date_year)).alias("distinct_claim_type_in_status_claim_date_year"),
        F.size(F.collect_set("claimed_amount_tier").over(w_claim_date_month)).alias("distinct_claimed_amount_tier_in_claim_date_month"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_date_month), F.lit(0)), 4).alias("approved_amount_share_of_claim_date_month"),
        F.min("claimed_amount").over(w_status_claim_date_year).alias("status_claim_date_year_min_claimed_amount"),
        F.first("claim_date").over(w_all_by_claim_date_full).alias("first_claim_date"),
        F.lag("claimed_amount", 1, 0).over(w_all_by_approved_amount).alias("prev_claimed_amount"),
        F.max("approved_amount").over(w_all_by_claim_date_moving_2).alias("approved_amount_max_3"),
        F.ntile(10).over(w_all_by_approved_amount).alias("approved_amount_decile"),
        F.count(F.lit(1)).over(w_status_claim_date_year).alias("rows_in_status_claim_date_year"),
    )
)

leaders = with_trends.filter(F.col("approved_amount_dense_rank") <= 5)

sequenced = (
    leaders
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "claim_date_month_max_approved_amount",
        "approved_amount_dense_rank",
        "distinct_claim_type_in_status_claim_date_year",
        "distinct_claimed_amount_tier_in_claim_date_month",
        "approved_amount_share_of_claim_date_month",
        "prev_claimed_amount",
        "approved_amount_max_3",
        "approved_amount_decile",
        F.rank().over(w_first_claim_date_month_polic).alias("distinct_claim_type_in_status_claim_date_year_ra"),
        F.round(F.percent_rank().over(w_first_claim_date_month_polic), 3).alias("distinct_claim_type_in_status_claim_date_year_pe"),
        F.nth_value("approved_amount_dense_rank", 2).over(w_first_claim_date_month_polic_2).alias("second_approved_amount_dense_rank"),
        F.round(F.avg("prev_claimed_amount").over(w_policy_end_date_year_approve), 2).alias("policy_end_date_year_approved_amount_dense_rank_"),
        F.size(F.collect_set("policy_id").over(w_policy_end_date_year_approve)).alias("distinct_policy_id_in_policy_end_date_year_appro"),
        F.lead("approved_amount_decile").over(w_approved_amount_dense_rank_s).alias("next_approved_amount_decile"),
        F.lead("next_approved_amount", 1, 0).over(w_first_claim_date_month_polic_3).alias("next_next_approved_amount"),
        F.round(F.avg("approved_amount_dense_rank").over(w_first_claim_date_month_polic_4), 2).alias("approved_amount_dense_rank_avg_4"),
    )
)

result = (
    sequenced
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "status",
        "claim_date_month_max_approved_amount",
        "distinct_claim_type_in_status_claim_date_year",
        "distinct_claimed_amount_tier_in_claim_date_month",
        "approved_amount_share_of_claim_date_month",
        "prev_claimed_amount",
        "distinct_claim_type_in_status_claim_date_year_ra",
        "distinct_claim_type_in_status_claim_date_year_pe",
        "second_approved_amount_dense_rank",
        "policy_end_date_year_approved_amount_dense_rank_",
        "distinct_policy_id_in_policy_end_date_year_appro",
        "next_approved_amount_decile",
        "next_next_approved_amount",
        "approved_amount_dense_rank_avg_4",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
    )
    .orderBy(F.col("approved_amount_dense_rank_avg_4").asc())
)
