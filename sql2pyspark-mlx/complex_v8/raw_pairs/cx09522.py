from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_month_by_claim_d = Window.partitionBy(F.month("reported_at")).orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_claim_type_by_re = Window.partitionBy("adjuster_id", "claim_type").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_adjuster_id_by_cla = Window.partitionBy("policy_id", "adjuster_id").orderBy("claimed_amount")
w_policy_id_adjuster_id_by_cla_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy("claimed_amount", "id")
w_claim_type_policy_id = Window.partitionBy("claim_type", "policy_id")
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_all_by_claim_date_ties = Window.orderBy("claim_date")
w_reported_at_month_by_claim_d_2 = Window.partitionBy(F.month("reported_at")).orderBy(F.col("claim_date").desc(), "id")
w_policy_id_status = Window.partitionBy("policy_id", "status")
w_adjuster_id_status_by_report = Window.partitionBy("adjuster_id", "status").orderBy("reported_at", "id")
w_policy_id_adjuster_id_by_cla_3 = Window.partitionBy("policy_id", "adjuster_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claimed_amount_dense_rank = Window.partitionBy("claimed_amount_dense_rank")
w_claimed_amount_quintile_repo = Window.partitionBy("claimed_amount_quintile", F.year("reported_at")).orderBy(F.col("claimed_amount_share_of_claim_type_policy_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claimed_amount_quintile_repo_2 = Window.partitionBy("claimed_amount_quintile", F.year("reported_at")).orderBy(F.col("claimed_amount_share_of_claim_type_policy_id").desc(), "id")

valid = (
    df_claims
    .filter((F.col("adjuster_id") <= 4) | (F.col("policy_id").isNotNull()))
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
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.count(F.lit(1)).over(w_reported_at_month_by_claim_d).alias("running_count"),
        F.count(F.lit(1)).over(w_adjuster_id_claim_type_by_re).alias("running_count_2"),
        F.dense_rank().over(w_policy_id_adjuster_id_by_cla).alias("claimed_amount_dense_rank"),
        F.ntile(5).over(w_policy_id_adjuster_id_by_cla_2).alias("claimed_amount_quintile"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_policy_id"),
        F.rank().over(w_all_by_claim_date).alias("claim_date_rank"),
        F.round(F.percent_rank().over(w_all_by_claim_date_ties), 3).alias("claim_date_percent_rank"),
        F.min("approved_amount").over(w_claim_type_policy_id).alias("claim_type_policy_id_min_approved_amount"),
        F.row_number().over(w_reported_at_month_by_claim_d_2).alias("rn"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_type_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_claim_type_policy_id"),
        F.size(F.collect_set("claim_type").over(w_policy_id_status)).alias("distinct_claim_type_in_policy_id_status"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_adjuster_id_status_by_report))) / 60, 1).alias("minutes_since_prev_reported_at"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_adjuster_id_by_cla_2)).alias("claimed_amount_change"),
        F.max("approved_amount").over(w_policy_id_adjuster_id_by_cla_3).alias("max_approved_amount_so_far"),
    )
)

derived = (
    with_windows
    .filter(F.col("status_clean").isNotNull())
    .withColumn("claim_type_len", F.length(F.trim(F.col("claim_type"))))
)

with_trends = (
    derived
    .select(
        "*",
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claimed_amount_dense_rank), F.lit(0)), 4).alias("claimed_amount_share_of_claimed_amount_dense_ran"),
        F.first("claimed_amount_quintile").over(w_claimed_amount_quintile_repo).alias("first_claimed_amount_quintile"),
        F.row_number().over(w_claimed_amount_quintile_repo_2).alias("seq_no"),
        F.round(F.col("claim_date_percent_rank") - F.avg("claim_date_percent_rank").over(w_claimed_amount_dense_rank), 2).alias("claim_date_percent_rank_vs_claimed_amount_dense_"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "status_clean",
        "running_count",
        "running_count_2",
        "claimed_amount_dense_rank",
        "claimed_amount_quintile",
        "approved_amount_share_of_claim_type_policy_id",
        "claim_date_percent_rank",
        "claim_type_policy_id_min_approved_amount",
        "rn",
        "claimed_amount_share_of_claim_type_policy_id",
        "minutes_since_prev_reported_at",
        "claimed_amount_change",
        "max_approved_amount_so_far",
        "claim_type_len",
        "claimed_amount_share_of_claimed_amount_dense_ran",
        "first_claimed_amount_quintile",
        "claim_date_percent_rank_vs_claimed_amount_dense_",
        F.round(F.col("running_count") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("running_count_per_approved_amount"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)
