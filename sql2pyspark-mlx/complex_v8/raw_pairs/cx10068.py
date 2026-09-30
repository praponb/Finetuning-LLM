from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_to_date_claim_ty = Window.partitionBy(F.to_date("reported_at"), "claim_type").orderBy("claim_date", "id")
w_reported_at_to_date_claim_ty_2 = Window.partitionBy(F.to_date("reported_at"), "claim_type").orderBy("claim_date")
w_policy_id = Window.partitionBy("policy_id")
w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_adjuster_id_policy_id_by_app = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-2, -1)
w_policy_id_claim_type_by_repo = Window.partitionBy("policy_id", "claim_type").orderBy(F.col("reported_at").desc())
w_adjuster_id_policy_id_by_app_2 = Window.partitionBy("adjuster_id", "policy_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_adjuster_id_in_poli = Window.partitionBy("distinct_adjuster_id_in_policy_id")
w_reported_at_month_distinct_a = Window.partitionBy(F.date_trunc("month", "reported_at"), "distinct_adjuster_id_in_policy_id").orderBy(F.col("reported_at").desc_nulls_first(), "id")
w_reported_at_to_date_distinct = Window.partitionBy(F.to_date("reported_at"), "distinct_policy_id_in_reported_at_to_date").orderBy("reported_at", "id")
w_reported_at_to_date_distinct_2 = Window.partitionBy(F.to_date("reported_at"), "distinct_policy_id_in_reported_at_to_date").orderBy("reported_at")

valid = (
    df_claims
    .filter(F.col("approved_amount") <= 32878.5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

windowed = (
    valid
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        F.lead("reported_at").over(w_reported_at_to_date_claim_ty).alias("next_reported_at"),
        F.round(F.percent_rank().over(w_reported_at_to_date_claim_ty_2), 3).alias("claim_date_percent_rank"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id)).alias("distinct_adjuster_id_in_policy_id"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_reported_at_to_date), 2).alias("approved_amount_vs_reported_at_to_date_avg"),
        F.round(F.avg("approved_amount").over(w_adjuster_id_policy_id_by_app), 2).alias("approved_amount_avg_2_prior"),
        F.size(F.collect_set("policy_id").over(w_reported_at_to_date)).alias("distinct_policy_id_in_reported_at_to_date"),
        F.dense_rank().over(w_policy_id_claim_type_by_repo).alias("reported_at_dense_rank"),
        F.last("reported_at").over(w_adjuster_id_policy_id_by_app_2).alias("last_reported_at"),
    )
)

leaders = windowed.filter(F.col("reported_at_dense_rank") <= 2)

ranked = (
    leaders
    .select(
        "id",
        "policy_id",
        "status",
        "next_reported_at",
        "claim_date_percent_rank",
        "distinct_adjuster_id_in_policy_id",
        "approved_amount_vs_reported_at_to_date_avg",
        "distinct_policy_id_in_reported_at_to_date",
        "reported_at_dense_rank",
        "last_reported_at",
        F.size(F.collect_set("adjuster_id").over(w_distinct_adjuster_id_in_poli)).alias("distinct_adjuster_id_in_distinct_adjuster_id_in_"),
        F.sum("reported_at_dense_rank").over(w_distinct_adjuster_id_in_poli).alias("distinct_adjuster_id_in_policy_id_sum_reported_a"),
        F.lag("claim_date_percent_rank", 2).over(w_reported_at_month_distinct_a).alias("prev2_claim_date_percent_rank"),
        F.ntile(4).over(w_reported_at_to_date_distinct).alias("reported_at_quartile"),
        F.lag("approved_amount_avg_2_prior", 3).over(w_reported_at_month_distinct_a).alias("prev3_approved_amount_avg_2_prior"),
        F.rank().over(w_reported_at_to_date_distinct_2).alias("reported_at_rank"),
    )
)

stats = (
    ranked
    .filter(F.col("last_reported_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .groupBy("reported_at_dense_rank", "reported_at_rank", "policy_id")
    .agg(
        F.sum(F.when(F.col("status") == "REJECTED", F.col("prev2_claim_date_percent_rank"))).alias("prev2_claim_date_percent_rank_status_rejected"),
        F.count("*").alias("row_count"),
        F.sum("prev2_claim_date_percent_rank").alias("total_prev2_claim_date_percent_rank"),
        F.max("reported_at_dense_rank").alias("max_reported_at_dense_rank_check"),
    )
    .filter(F.col("max_reported_at_dense_rank_check") > 2)
    .drop("max_reported_at_dense_rank_check")
)

result = (
    stats
    .select(
        "reported_at_dense_rank",
        "reported_at_rank",
        "policy_id",
        "row_count",
        F.coalesce(F.col("reported_at_rank"), F.lit(0)).alias("reported_at_rank_filled"),
    )
    .orderBy(F.col("policy_id").asc())
)
