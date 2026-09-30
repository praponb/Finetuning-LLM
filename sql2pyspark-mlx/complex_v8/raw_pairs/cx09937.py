from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_repo = Window.partitionBy("policy_id", "claim_type").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_status = Window.partitionBy("policy_id", "status")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_claim_date_month_claim_type = Window.partitionBy(F.date_trunc("month", "claim_date"), "claim_type").orderBy("claim_date", "id").rowsBetween(-1, Window.currentRow)
w_policy_id_claim_type_by_repo_2 = Window.partitionBy("policy_id", "claim_type").orderBy("reported_at", "id")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy("claimed_amount", "id").rowsBetween(-1, -1)
w_claim_type_status = Window.partitionBy("claim_type", "status")
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount", "id")
w_claim_type_policy_id_by_clai_2 = Window.partitionBy("claim_type", "policy_id").orderBy("claimed_amount", "id")
w_policy_id_claim_type_by_repo_3 = Window.partitionBy("policy_id", "claim_type").orderBy("reported_at", "id").rowsBetween(-1, -1)
w_rn_adjuster_id_by_claimed_am = Window.partitionBy("rn", "adjuster_id").orderBy("claimed_amount_vs_claim_type_status_avg", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")

recent = (
    df_claims
    .filter(F.year(F.col("claim_date")) == 2025)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
    )
)

sequenced = (
    recent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        "status_clean",
        F.max("claimed_amount").over(w_policy_id_claim_type_by_repo).alias("max_claimed_amount_so_far"),
        F.round(F.avg("claimed_amount").over(w_policy_id_status), 2).alias("policy_id_status_avg_claimed_amount"),
        F.round(F.avg("claimed_amount").over(w_status_adjuster_id), 2).alias("status_adjuster_id_avg_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_adjuster_id), 2).alias("claimed_amount_vs_status_adjuster_id_avg"),
        F.sum("claimed_amount").over(w_claim_date_month_claim_type).alias("claimed_amount_sum_2"),
        F.row_number().over(w_policy_id_claim_type_by_repo_2).alias("rn"),
        F.sum("claimed_amount").over(w_policy_id_status).alias("policy_id_status_sum_claimed_amount"),
        F.row_number().over(w_policy_id_by_claim_date).alias("claim_date_seq"),
        F.sum("claimed_amount").over(w_claim_type_policy_id_by_clai).alias("claimed_amount_sum_1_prior"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type_status), 2).alias("claimed_amount_vs_claim_type_status_avg"),
        F.size(F.collect_set("policy_id").over(w_status_adjuster_id)).alias("distinct_policy_id_in_status_adjuster_id"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_type_status), F.lit(0)), 4).alias("claimed_amount_share_of_claim_type_status"),
        F.ntile(3).over(w_claim_type_by_claimed_amount).alias("claimed_amount_tercile"),
        F.round(F.cume_dist().over(w_claim_type_policy_id_by_clai_2), 3).alias("claimed_amount_cume_dist"),
        F.max("claimed_amount").over(w_policy_id_claim_type_by_repo_3).alias("claimed_amount_max_1_prior"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.rank().over(w_rn_adjuster_id_by_claimed_am).alias("claimed_amount_vs_claim_type_status_avg_rank"),
        F.round(F.col("claimed_amount_max_1_prior") / F.nullif(F.sum("claimed_amount_max_1_prior").over(w_adjuster_id), F.lit(0)), 4).alias("claimed_amount_max_1_prior_share_of_adjuster_id"),
        F.round(F.col("claimed_amount_tercile") / F.nullif(F.sum("claimed_amount_tercile").over(w_adjuster_id), F.lit(0)), 4).alias("claimed_amount_tercile_share_of_adjuster_id"),
        F.round(F.avg("rn").over(w_adjuster_id), 2).alias("adjuster_id_avg_rn"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "policy_id",
        "claim_type",
        "reported_at",
        "policy_id_status_avg_claimed_amount",
        "status_adjuster_id_avg_claimed_amount",
        "claimed_amount_vs_status_adjuster_id_avg",
        "rn",
        "policy_id_status_sum_claimed_amount",
        "claim_date_seq",
        "claimed_amount_vs_claim_type_status_avg",
        "claimed_amount_share_of_claim_type_status",
        "claimed_amount_tercile",
        "claimed_amount_cume_dist",
        "claimed_amount_vs_claim_type_status_avg_rank",
        "claimed_amount_max_1_prior_share_of_adjuster_id",
        "claimed_amount_tercile_share_of_adjuster_id",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.round(F.col("claimed_amount_tercile") / F.nullif(F.col("claimed_amount_sum_2"), F.lit(0)), 3).alias("claimed_amount_tercile_per_claimed_amount_sum_2"),
    )
    .orderBy(
        F.col("claimed_amount_tercile").desc(),
        F.col("claimed_amount_tercile_share_of_adjuster_id").desc(),
        "id",
    )
    .limit(10)
)
