from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_reported_at_prior = Window.partitionBy("status").orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy("reported_at", "id")
w_status_by_reported_at = Window.partitionBy("status").orderBy(F.col("reported_at").asc_nulls_last(), "id")
w_status_by_reported_at_full = Window.partitionBy("status").orderBy(F.col("reported_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_reported_at_ti = Window.partitionBy("claim_type").orderBy("reported_at")
w_reported_at_year_adjuster_id = Window.partitionBy(F.year("reported_at"), "adjuster_id").orderBy("reported_at")
w_claim_type_by_reported_at_ru = Window.partitionBy("claim_type").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_reported_at_fu = Window.partitionBy("claim_type").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_year_adjuster_id_2 = Window.partitionBy(F.year("reported_at"), "adjuster_id").orderBy("reported_at", "id")
w_all_by_total_adjuster_id_sum = Window.orderBy("total_adjuster_id_sum_approved_amount", "last_claim_date_quarter_ym", "next_reported_at_ym", "status").rowsBetween(-1, -1)
w_status_n_claim_type_flood = Window.partitionBy("status", "n_claim_type_flood")

valid = (
    df_claims
    .filter(F.col("policy_id") <= 16)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_trunc("quarter", F.col("claim_date")).alias("claim_date_quarter"),
        F.date_format(F.col("claim_date"), "MMM yyyy").alias("claim_date_label"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.sum("approved_amount").over(w_status_by_reported_at_prior).alias("approved_amount_sum_6_prior"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.lag("claim_date").over(w_claim_type_by_reported_at).alias("prev_claim_date"),
        F.min("approved_amount").over(w_adjuster_id).alias("adjuster_id_min_approved_amount"),
        F.lead("reported_at").over(w_status_by_reported_at).alias("next_reported_at"),
        F.first("claim_date_quarter").over(w_status_by_reported_at_full).alias("first_claim_date_quarter"),
        F.dense_rank().over(w_claim_type_by_reported_at_ti).alias("reported_at_dense_rank"),
        F.dense_rank().over(w_reported_at_year_adjuster_id).alias("reported_at_dense_rank_2"),
        F.sum("approved_amount").over(w_adjuster_id).alias("adjuster_id_sum_approved_amount"),
        F.max("approved_amount").over(w_claim_type_by_reported_at_ru).alias("max_approved_amount_so_far"),
        F.first("claim_date_quarter").over(w_claim_type_by_reported_at_fu).alias("first_claim_date_quarter_2"),
        F.last("claim_date_quarter").over(w_claim_type_by_reported_at_fu).alias("last_claim_date_quarter"),
        F.lead("claim_date", 3).over(w_reported_at_year_adjuster_id_2).alias("next3_claim_date"),
    )
)

enriched = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at",
        "approved_amount_sum_6_prior",
        "rows_in_adjuster_id",
        "distinct_policy_id_in_adjuster_id",
        "prev_claim_date",
        "adjuster_id_min_approved_amount",
        "next_reported_at",
        "first_claim_date_quarter",
        "reported_at_dense_rank",
        "reported_at_dense_rank_2",
        "adjuster_id_sum_approved_amount",
        "first_claim_date_quarter_2",
        "last_claim_date_quarter",
        "next3_claim_date",
        F.greatest(F.col("rows_in_adjuster_id"), F.col("reported_at_dense_rank")).alias("greatest_rows_in_adjuster_id_reported_at_dense_r"),
    )
)

grouped = (
    enriched
    .filter(F.col("next3_claim_date") < F.lit("2025-05-30").cast("date"))
    .groupBy(
        F.date_format("last_claim_date_quarter", "yyyy-MM").alias("last_claim_date_quarter_ym"),
        F.date_format("next_reported_at", "yyyy-MM").alias("next_reported_at_ym"),
        "status",
    )
    .agg(
        F.sum("adjuster_id_sum_approved_amount").alias("total_adjuster_id_sum_approved_amount"),
        F.count(F.when(F.col("claim_type") == "flood", 1)).alias("n_claim_type_flood"),
        F.countDistinct("adjuster_id").alias("distinct_adjuster_ids"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.sum(F.when(F.col("claim_type") == "flood", F.col("reported_at_dense_rank_2"))).alias("reported_at_dense_rank_2_claim_type_flood"),
        F.sum(F.when(F.col("status").isin("APPROVED", "IN_REVIEW", "PAID"), 1).otherwise(0)).alias("n_status_in_approved_in_review"),
        F.sum("reported_at_dense_rank").alias("sum_reported_at_dense_rank_check"),
    )
    .filter(F.col("sum_reported_at_dense_rank_check") > 2)
    .drop("sum_reported_at_dense_rank_check")
)

windowed = (
    grouped
    .select(
        "last_claim_date_quarter_ym",
        "next_reported_at_ym",
        "status",
        "total_adjuster_id_sum_approved_amount",
        "n_claim_type_flood",
        "claim_type_list",
        F.round(F.avg("distinct_adjuster_ids").over(w_all_by_total_adjuster_id_sum), 2).alias("distinct_adjuster_ids_avg_1_prior"),
        F.round(F.col("distinct_adjuster_ids") / F.nullif(F.sum("distinct_adjuster_ids").over(w_status_n_claim_type_flood), F.lit(0)), 4).alias("distinct_adjuster_ids_share_of_status_n_claim_ty"),
    )
)

result = (
    windowed
    .select(
        "last_claim_date_quarter_ym",
        "next_reported_at_ym",
        "status",
        "total_adjuster_id_sum_approved_amount",
        "n_claim_type_flood",
        "distinct_adjuster_ids_avg_1_prior",
        "distinct_adjuster_ids_share_of_status_n_claim_ty",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
    .orderBy(F.col("next_reported_at_ym").desc())
)
