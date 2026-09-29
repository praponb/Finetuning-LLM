from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_status_adjuster_id_by_claime = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_adjuster_id_policy_id_by_app = Window.partitionBy("adjuster_id", "policy_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_adjuster_id_by_approv = Window.partitionBy("status", "adjuster_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-3, -1)
w_adjuster_id_policy_id_by_app_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy("approved_amount", "id").rowsBetween(-1, -1)
w_status_adjuster_id_by_claime_2 = Window.partitionBy("status", "adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_policy_id_by_app_3 = Window.partitionBy("adjuster_id", "policy_id").orderBy("approved_amount")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount", "id")
w_claim_type_by_claimed_amount_2 = Window.partitionBy("claim_type").orderBy("claimed_amount")
w_all_by_approved_amount_avg_3 = Window.orderBy(F.col("approved_amount_avg_3_prior").desc(), "id").rowsBetween(-2, -1)
w_status = Window.partitionBy("status")
w_distinct_ids_by_distinct_ids = Window.partitionBy("distinct_ids").orderBy(F.col("distinct_ids").asc_nulls_last(), "adjuster_team", "claimed_amount_quartile")
w_distinct_ids_by_distinct_ids_2 = Window.partitionBy("distinct_ids").orderBy(F.col("distinct_ids").asc_nulls_last())
w_distinct_ids_by_distinct_ids_3 = Window.partitionBy("distinct_ids").orderBy(F.col("distinct_ids").asc_nulls_last(), "adjuster_team", "claimed_amount_quartile").rowsBetween(-1, -1)

valid_claims = (
    df_claims
    .filter(F.col("policy_id").isNotNull())
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

combined_details_adjuster = (
    valid_claims.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.team").isin("Travel", "Health"))
    .select("s.*", F.col("a.team").alias("adjuster_team"))
)

sequenced = (
    combined_details_adjuster
    .select(
        "*",
        F.min("claimed_amount").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_min_claimed_amount"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_status_adjuster_id_by_claime)).alias("approved_amount_change"),
        F.max("approved_amount").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_max_approved_amount"),
        F.sum("approved_amount").over(w_adjuster_id_policy_id_by_app).alias("running_approved_amount"),
        F.row_number().over(w_status_adjuster_id_by_claime).alias("claimed_amount_seq"),
        F.round(F.avg("approved_amount").over(w_status_adjuster_id_by_approv), 2).alias("approved_amount_avg_3_prior"),
        F.round(F.avg("claimed_amount").over(w_adjuster_id_policy_id_by_app_2), 2).alias("claimed_amount_avg_1_prior"),
        F.last("approved_amount", ignorenulls=True).over(w_status_adjuster_id_by_claime_2).alias("last_known_approved_amount"),
        F.dense_rank().over(w_adjuster_id_policy_id_by_app_3).alias("approved_amount_dense_rank"),
        F.first("claimed_amount", ignorenulls=True).over(w_status_adjuster_id_by_claime_2).alias("first_known_claimed_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("approved_amount_vs_adjuster_id_policy_id_avg"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "adjuster_team",
        "approved_amount_change",
        "adjuster_id_policy_id_max_approved_amount",
        "running_approved_amount",
        "approved_amount_avg_3_prior",
        "claimed_amount_avg_1_prior",
        "approved_amount_dense_rank",
        "first_known_claimed_amount",
        "approved_amount_vs_adjuster_id_policy_id_avg",
        F.ntile(4).over(w_claim_type_by_claimed_amount).alias("claimed_amount_quartile"),
        F.round(F.cume_dist().over(w_claim_type_by_claimed_amount_2), 3).alias("claimed_amount_cume_dist"),
        F.round(F.avg("claimed_amount_avg_1_prior").over(w_all_by_approved_amount_avg_3), 2).alias("claimed_amount_avg_1_prior_avg_2_prior"),
        F.max("approved_amount_change").over(w_status).alias("status_max_approved_amount_change"),
        F.sum("approved_amount_dense_rank").over(w_status).alias("status_sum_approved_amount_dense_rank"),
    )
)

per_group = (
    with_trends
    .filter(F.col("approved_amount_change") <= 10000.0)
    .groupBy("adjuster_team", "claimed_amount_quartile")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("status").isin("REJECTED", "PAID", "IN_REVIEW"), 1).otherwise(0)).alias("n_status_in_rejected_paid_in_r"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("claimed_amount_quartile"), 2).alias("avg_claimed_amount_quartile"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_windows = (
    per_group
    .select(
        "*",
        F.row_number().over(w_distinct_ids_by_distinct_ids).alias("distinct_ids_seq"),
        F.round(F.cume_dist().over(w_distinct_ids_by_distinct_ids_2), 3).alias("distinct_ids_cume_dist"),
        F.round(F.avg("avg_claimed_amount_quartile").over(w_distinct_ids_by_distinct_ids_3), 2).alias("avg_claimed_amount_quartile_avg_1_prior"),
    )
)

result = (
    with_windows
    .select(
        "adjuster_team",
        "claimed_amount_quartile",
        "n_status_in_rejected_paid_in_r",
        "avg_claimed_amount_quartile",
        "avg_claimed_amount_quartile_avg_1_prior",
        F.lower(F.trim(F.col("adjuster_team"))).alias("adjuster_team_clean"),
        (F.col("distinct_ids") - F.col("distinct_ids_cume_dist")).alias("distinct_ids_minus_distinct_ids_cume_dist"),
    )
    .orderBy("avg_claimed_amount_quartile")
)
