from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type = Window.partitionBy("policy_id", "claim_type")
w_adjuster_level_by_policy_pre = Window.partitionBy("adjuster_level").orderBy(F.col("policy_premium").desc_nulls_first())
w_claim_type_policy_status_by = Window.partitionBy("claim_type", "policy_status").orderBy("policy_premium", "id")
w_reported_at_year_by_claimed = Window.partitionBy(F.year("reported_at")).orderBy("claimed_amount_adj", "id")
w_reported_at_year_by_claimed_2 = Window.partitionBy(F.year("reported_at")).orderBy("claimed_amount_adj", "id").rowsBetween(-3, -1)
w_adjuster_level_by_policy_pre_2 = Window.partitionBy("adjuster_level").orderBy(F.col("policy_premium").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_premium_rank_rn = Window.partitionBy("policy_premium_rank", "rn")
w_policy_status_by_policy_id_c = Window.partitionBy("policy_status").orderBy(F.col("policy_id_claim_type_avg_adjuster_level").desc(), "id")
w_all_by_policy_id_claim_type = Window.orderBy("policy_id_claim_type_max_adjuster_level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_rn_policy_status_by_policy_p = Window.partitionBy("rn", "policy_status").orderBy("policy_premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn_policy_status_by_policy_p_2 = Window.partitionBy("rn", "policy_status").orderBy("policy_premium")
w_policy_status_rn = Window.partitionBy("policy_status", "rn")

filtered = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.round(F.col("claimed_amount") * 0.25, 2).alias("claimed_amount_adj"),
    )
)

with_details_policy = (
    filtered.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_date_label",
        "s.claimed_amount_adj",
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
        F.col("a.level").alias("adjuster_level"),
    )
)

metrics = (
    with_details_policy
    .select(
        "id",
        "claim_type",
        "reported_at",
        "claimed_amount_adj",
        "policy_premium",
        "policy_status",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_claim_type"),
        F.rank().over(w_adjuster_level_by_policy_pre).alias("policy_premium_rank"),
        F.lag("approved_amount").over(w_claim_type_policy_status_by).alias("prev_approved_amount"),
        F.row_number().over(w_reported_at_year_by_claimed).alias("rn"),
        F.max("adjuster_level").over(w_policy_id_claim_type).alias("policy_id_claim_type_max_adjuster_level"),
        F.sum("policy_premium").over(w_reported_at_year_by_claimed_2).alias("policy_premium_sum_3_prior"),
        F.round(F.avg("adjuster_level").over(w_policy_id_claim_type), 2).alias("policy_id_claim_type_avg_adjuster_level"),
        F.round(F.col("claimed_amount_adj") / F.nullif(F.sum("claimed_amount_adj").over(w_policy_id_claim_type), F.lit(0)), 4).alias("claimed_amount_adj_share_of_policy_id_claim_type"),
        F.max("policy_premium").over(w_adjuster_level_by_policy_pre_2).alias("max_policy_premium_so_far"),
        F.count(F.lit(1)).over(w_policy_id_claim_type).alias("rows_in_policy_id_claim_type"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.round(F.avg("rows_in_policy_id_claim_type").over(w_policy_premium_rank_rn), 2).alias("policy_premium_rank_rn_avg_rows_in_policy_id_cla"),
        F.dense_rank().over(w_policy_status_by_policy_id_c).alias("policy_id_claim_type_avg_adjuster_level_dense_ra"),
        F.sum("policy_premium_sum_3_prior").over(w_all_by_policy_id_claim_type).alias("running_policy_premium_sum_3_prior"),
        F.ntile(10).over(w_policy_status_by_policy_id_c).alias("policy_id_claim_type_avg_adjuster_level_decile"),
        F.nth_value("policy_premium_sum_3_prior", 2).over(w_rn_policy_status_by_policy_p).alias("second_policy_premium_sum_3_prior"),
        F.round(F.percent_rank().over(w_rn_policy_status_by_policy_p_2), 3).alias("policy_premium_percent_rank"),
        F.round(F.avg("prev_approved_amount").over(w_policy_status_rn), 2).alias("policy_status_rn_avg_prev_approved_amount"),
        F.size(F.collect_set("claim_type").over(w_policy_status_rn)).alias("distinct_claim_type_in_policy_status_rn"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "claim_type",
        "reported_at",
        "claimed_amount_adj",
        "policy_premium",
        "policy_status",
        "approved_amount_share_of_policy_id_claim_type",
        "policy_premium_rank",
        "prev_approved_amount",
        "rn",
        "policy_id_claim_type_max_adjuster_level",
        "policy_premium_sum_3_prior",
        "policy_id_claim_type_avg_adjuster_level",
        "claimed_amount_adj_share_of_policy_id_claim_type",
        "max_policy_premium_so_far",
        "rows_in_policy_id_claim_type",
        "policy_premium_rank_rn_avg_rows_in_policy_id_cla",
        "policy_id_claim_type_avg_adjuster_level_dense_ra",
        "policy_id_claim_type_avg_adjuster_level_decile",
        "policy_premium_percent_rank",
        "policy_status_rn_avg_prev_approved_amount",
        "distinct_claim_type_in_policy_status_rn",
        F.greatest(F.col("prev_approved_amount"), F.col("policy_premium_rank")).alias("greatest_prev_approved_amount_policy_premium_ran"),
        F.when(F.col("claimed_amount_adj") >= 41295.0, "HIGH").when(F.col("claimed_amount_adj") >= 26315.0, "MEDIUM").otherwise("LOW").alias("claimed_amount_adj_tier"),
    )
)
