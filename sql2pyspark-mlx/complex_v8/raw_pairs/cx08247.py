from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type = Window.partitionBy("policy_id", "claim_type")
w_adjuster_id_claim_type = Window.partitionBy("adjuster_id", "claim_type")
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_claim_type_by_appr = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount", "id")
w_policy_id = Window.partitionBy("policy_id")
w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_policy_id_claim_type_by_appr_2 = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").asc_nulls_last(), "id")
w_policy_id_claim_type_by_appr_3 = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount", "id").rowsBetween(-2, Window.currentRow)
w_distinct_claim_type_in_polic = Window.partitionBy("distinct_claim_type_in_policy_id", "adjuster_id")
w_approved_amount_rank_by_dist = Window.partitionBy("approved_amount_rank").orderBy("distinct_adjuster_id_in_policy_id_claim_type")
w_distinct_adjuster_id_in_poli = Window.partitionBy("distinct_adjuster_id_in_policy_id_claim_type", "distinct_policy_id_in_adjuster_id_claim_type").orderBy("next_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.size(F.collect_set("adjuster_id").over(w_policy_id_claim_type)).alias("distinct_adjuster_id_in_policy_id_claim_type"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id_claim_type)).alias("distinct_policy_id_in_adjuster_id_claim_type"),
        F.min("approved_amount").over(w_claim_type_by_approved_amoun).alias("min_approved_amount_so_far"),
        F.ntile(4).over(w_policy_id_claim_type_by_appr).alias("approved_amount_quartile"),
        F.size(F.collect_set("claim_type").over(w_policy_id)).alias("distinct_claim_type_in_policy_id"),
        F.lead("claimed_amount", 1, 0).over(w_all_by_claimed_amount).alias("next_claimed_amount"),
        F.max("approved_amount").over(w_policy_id_claim_type).alias("policy_id_claim_type_max_approved_amount"),
        F.rank().over(w_policy_id_claim_type_by_appr_2).alias("approved_amount_rank"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.rank().over(w_all_by_claimed_amount).alias("claimed_amount_rank"),
        F.round(F.cume_dist().over(w_policy_id_claim_type_by_appr), 3).alias("approved_amount_cume_dist"),
        F.lag("approved_amount").over(w_claim_type_by_claimed_amount).alias("prev_approved_amount"),
        F.round(F.percent_rank().over(w_all_by_approved_amount), 3).alias("approved_amount_percent_rank"),
        F.round(F.avg("approved_amount").over(w_policy_id_claim_type_by_appr_3), 2).alias("approved_amount_avg_3"),
    )
)

with_trends = (
    metrics
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "approved_amount",
        "claim_type_slug",
        "distinct_adjuster_id_in_policy_id_claim_type",
        "distinct_policy_id_in_adjuster_id_claim_type",
        "min_approved_amount_so_far",
        "next_claimed_amount",
        "policy_id_claim_type_max_approved_amount",
        "claimed_amount_rank",
        "prev_approved_amount",
        "approved_amount_avg_3",
        F.round(F.col("approved_amount_cume_dist") / F.nullif(F.sum("approved_amount_cume_dist").over(w_distinct_claim_type_in_polic), F.lit(0)), 4).alias("approved_amount_cume_dist_share_of_distinct_clai"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_distinct_claim_type_in_polic), 2).alias("approved_amount_vs_distinct_claim_type_in_policy"),
        F.round(F.percent_rank().over(w_approved_amount_rank_by_dist), 3).alias("distinct_adjuster_id_in_policy_id_claim_type_per"),
        F.round(F.col("min_approved_amount_so_far") / F.nullif(F.sum("min_approved_amount_so_far").over(w_distinct_claim_type_in_polic), F.lit(0)), 4).alias("min_approved_amount_so_far_share_of_distinct_cla"),
        F.count(F.lit(1)).over(w_distinct_claim_type_in_polic).alias("rows_in_distinct_claim_type_in_policy_id_adjuste"),
        F.last("claimed_amount").over(w_distinct_adjuster_id_in_poli).alias("last_claimed_amount"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "claim_type_slug",
        "distinct_policy_id_in_adjuster_id_claim_type",
        "min_approved_amount_so_far",
        "next_claimed_amount",
        "policy_id_claim_type_max_approved_amount",
        "claimed_amount_rank",
        "prev_approved_amount",
        "approved_amount_avg_3",
        "approved_amount_cume_dist_share_of_distinct_clai",
        "approved_amount_vs_distinct_claim_type_in_policy",
        "distinct_adjuster_id_in_policy_id_claim_type_per",
        "min_approved_amount_so_far_share_of_distinct_cla",
        "rows_in_distinct_claim_type_in_policy_id_adjuste",
    )
    .orderBy("policy_id_claim_type_max_approved_amount")
)
