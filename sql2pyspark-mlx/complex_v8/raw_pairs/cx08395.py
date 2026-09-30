from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_full = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_reported_at_full_2 = Window.orderBy(F.col("reported_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_policy_id = Window.partitionBy("claim_type", "policy_id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_all_by_reported_at_running = Window.orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_reported_at_running_2 = Window.orderBy(F.col("reported_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_reported_at_to_date_b = Window.partitionBy("status", F.to_date("reported_at")).orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_reported_at_to_date_b_2 = Window.partitionBy("status", F.to_date("reported_at")).orderBy("approved_amount", "id")
w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_claim_type_reported_at_year = Window.partitionBy("claim_type", F.year("reported_at"))
w_approved_amount_tier_status = Window.partitionBy("approved_amount_tier", "status").orderBy(F.col("claim_date").desc(), "id")
w_approved_amount_tier_status_2 = Window.partitionBy("approved_amount_tier", "status").orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_dense_rank_polic = Window.partitionBy("reported_at_dense_rank", "policy_id")
w_last_known_reported_at_month = Window.partitionBy(F.date_trunc("month", "last_known_reported_at")).orderBy("first_reported_at_2", "id").rowsBetween(-3, -1)
w_last_known_reported_at_month_2 = Window.partitionBy(F.date_trunc("month", "last_known_reported_at")).orderBy("first_reported_at_2")
w_last_known_reported_at_month_3 = Window.partitionBy(F.date_trunc("month", "last_known_reported_at")).orderBy("first_reported_at_2", "id")

prepared = (
    df_claims
    .filter(F.col("claim_date").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 14668.0, "large").when(F.col("approved_amount") >= 2887.3, "medium").otherwise("small").alias("approved_amount_tier"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "approved_amount_tier",
        F.first("reported_at").over(w_all_by_reported_at_full).alias("first_reported_at"),
        F.first("approved_amount").over(w_all_by_reported_at_full_2).alias("first_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type_policy_id), 2).alias("approved_amount_vs_claim_type_policy_id_avg"),
        F.dense_rank().over(w_all_by_reported_at).alias("reported_at_dense_rank"),
        F.last("approved_amount", ignorenulls=True).over(w_all_by_reported_at_running).alias("last_known_approved_amount"),
        F.min("approved_amount").over(w_all_by_reported_at_running_2).alias("min_approved_amount_so_far"),
        F.first("reported_at").over(w_status_reported_at_to_date_b).alias("first_reported_at_2"),
        F.lag("claim_date", 2).over(w_status_reported_at_to_date_b_2).alias("prev2_claim_date"),
        F.round(F.avg("approved_amount").over(w_reported_at_to_date), 2).alias("reported_at_to_date_avg_approved_amount"),
        F.last("reported_at", ignorenulls=True).over(w_all_by_reported_at_running).alias("last_known_reported_at"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_reported_at_year), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_reported_at_"),
        F.dense_rank().over(w_approved_amount_tier_status).alias("claim_date_dense_rank"),
        F.first("reported_at").over(w_approved_amount_tier_status_2).alias("first_reported_at_3"),
    )
)

top_per_group = (
    metrics
    .filter((F.col("claim_date_dense_rank") == 1) & (F.col("first_reported_at_2") >= F.lit("2025-02-06 09:00:00").cast("timestamp")))
    .drop("claim_date_dense_rank")
)

with_trends = (
    top_per_group
    .select(
        "*",
        F.round(F.col("first_approved_amount") - F.avg("first_approved_amount").over(w_reported_at_dense_rank_polic), 2).alias("first_approved_amount_vs_reported_at_dense_rank_"),
        F.round(F.avg("last_known_approved_amount").over(w_last_known_reported_at_month), 2).alias("last_known_approved_amount_avg_3_prior"),
        F.round(F.cume_dist().over(w_last_known_reported_at_month_2), 3).alias("first_reported_at_2_cume_dist"),
        F.max("min_approved_amount_so_far").over(w_reported_at_dense_rank_polic).alias("reported_at_dense_rank_policy_id_max_min_approve"),
        F.rank().over(w_last_known_reported_at_month_3).alias("first_reported_at_2_rank"),
        F.lead("last_known_approved_amount", 1, 0).over(w_last_known_reported_at_month_3).alias("next_last_known_approved_amount"),
        F.lag("first_reported_at", 3).over(w_last_known_reported_at_month_3).alias("prev3_first_reported_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "first_reported_at",
        "approved_amount_vs_claim_type_policy_id_avg",
        "reported_at_dense_rank",
        "min_approved_amount_so_far",
        "last_known_reported_at",
        "first_reported_at_3",
        "first_approved_amount_vs_reported_at_dense_rank_",
        "last_known_approved_amount_avg_3_prior",
        "first_reported_at_2_cume_dist",
        "reported_at_dense_rank_policy_id_max_min_approve",
        "first_reported_at_2_rank",
        "next_last_known_approved_amount",
    )
    .orderBy(F.col("id").desc())
)
