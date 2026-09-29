from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_policy_id_by = Window.partitionBy(F.year("claim_date"), "policy_id").orderBy("reported_at", "id").rowsBetween(-1, -1)
w_claim_date_year_policy_id_by_2 = Window.partitionBy(F.year("claim_date"), "policy_id").orderBy("reported_at", "id")
w_claim_type_policy_id = Window.partitionBy("claim_type", "policy_id")
w_claim_type_status_by_reporte = Window.partitionBy("claim_type", "status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_reported_at = Window.partitionBy("status").orderBy("reported_at", "id")
w_all_by_reported_at_prior_3 = Window.orderBy("reported_at", "id").rowsBetween(-3, -1)
w_claim_type_status_by_reporte_2 = Window.partitionBy("claim_type", "status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_status_by_reporte_3 = Window.partitionBy("claim_type", "status").orderBy("reported_at", "id")
w_status_policy_id_by_reported = Window.partitionBy("status", "policy_id").orderBy(F.col("reported_at_cume_dist").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_second_reported_at_to_date_r = Window.partitionBy(F.to_date("second_reported_at"), "reported_at_dense_rank")
w_status_policy_id_by_reported_2 = Window.partitionBy("status", "policy_id").orderBy(F.col("reported_at_cume_dist").asc_nulls_last(), "id")
w_distinct_policy_ids_policy_i = Window.partitionBy("distinct_policy_ids", "policy_id").orderBy(F.col("second_reported_at_to_date_reported_at_dense_ran_2").asc_nulls_last())
w_policy_id_cnt = Window.partitionBy("policy_id", "cnt")
w_distinct_policy_ids_policy_i_2 = Window.partitionBy("distinct_policy_ids", "policy_id").orderBy(F.col("second_reported_at_to_date_reported_at_dense_ran_2").asc_nulls_last(), "policy_id")
w_distinct_policy_ids_policy_i_3 = Window.partitionBy("distinct_policy_ids", "policy_id").orderBy(F.col("second_reported_at_to_date_reported_at_dense_ran_2").asc_nulls_last(), "policy_id").rowsBetween(-1, -1)

base = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2024-10-22").cast("date")) & (F.col("policy_id") <= 28))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

metrics = (
    base
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "approved_amount_minus_claimed_amount",
        F.max("claimed_amount").over(w_claim_date_year_policy_id_by).alias("claimed_amount_max_1_prior"),
        F.dense_rank().over(w_claim_date_year_policy_id_by_2).alias("reported_at_dense_rank"),
        F.count(F.lit(1)).over(w_claim_type_policy_id).alias("rows_in_claim_type_policy_id"),
        F.round(F.cume_dist().over(w_claim_date_year_policy_id_by_2), 3).alias("reported_at_cume_dist"),
        F.count(F.lit(1)).over(w_claim_type_status_by_reporte).alias("running_count"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_date_year_policy_id_by_2)).alias("days_since_prev_claim_date"),
        F.row_number().over(w_status_by_reported_at).alias("reported_at_seq"),
        F.sum("claimed_amount").over(w_all_by_reported_at_prior_3).alias("claimed_amount_sum_3_prior"),
        F.lead("claimed_amount", 3, 0).over(w_status_by_reported_at).alias("next3_claimed_amount"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type_policy_id), 2).alias("claimed_amount_vs_claim_type_policy_id_avg"),
        F.nth_value("reported_at", 2).over(w_claim_type_status_by_reporte_2).alias("second_reported_at"),
        F.round(F.cume_dist().over(w_claim_type_status_by_reporte_3), 3).alias("reported_at_cume_dist_2"),
    )
)

top_rows = (
    metrics
    .filter(F.col("reported_at_seq") <= 2)
    .drop("reported_at_seq")
)

with_windows = (
    top_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "approved_amount_minus_claimed_amount",
        "claimed_amount_max_1_prior",
        "reported_at_dense_rank",
        "rows_in_claim_type_policy_id",
        "reported_at_cume_dist",
        "days_since_prev_claim_date",
        "claimed_amount_sum_3_prior",
        "next3_claimed_amount",
        "claimed_amount_vs_claim_type_policy_id_avg",
        "second_reported_at",
        "reported_at_cume_dist_2",
        F.sum("claimed_amount").over(w_status_policy_id_by_reported).alias("claimed_amount_sum_7"),
        F.count(F.lit(1)).over(w_second_reported_at_to_date_r).alias("rows_in_second_reported_at_to_date_reported_at_d"),
        F.ntile(5).over(w_status_policy_id_by_reported_2).alias("reported_at_cume_dist_quintile"),
        F.round(F.col("reported_at_cume_dist") - F.avg("reported_at_cume_dist").over(w_second_reported_at_to_date_r), 2).alias("reported_at_cume_dist_vs_second_reported_at_to_d"),
        F.min("reported_at_dense_rank").over(w_second_reported_at_to_date_r).alias("second_reported_at_to_date_reported_at_dense_ran"),
    )
)

per_group = (
    with_windows
    .filter(F.col("approved_amount_minus_claimed_amount") >= 25000.0)
    .groupBy("policy_id")
    .agg(
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.min("second_reported_at_to_date_reported_at_dense_ran").alias("min_second_reported_at_to_date_reported_at_dense"),
        F.count("*").alias("cnt"),
        F.round(F.sum("next3_claimed_amount") / F.nullif(F.sum("second_reported_at_to_date_reported_at_dense_ran"), F.lit(0)), 3).alias("next3_claimed_amount_to_second_reported_at_to_da"),
        F.round(F.sum("second_reported_at_to_date_reported_at_dense_ran") / F.nullif(F.sum("rows_in_claim_type_policy_id"), F.lit(0)), 3).alias("second_reported_at_to_date_reported_at_dense_ran_2"),
        F.max("claimed_amount_sum_3_prior").alias("max_claimed_amount_sum_3_prior_check"),
    )
    .filter(F.col("max_claimed_amount_sum_3_prior_check") > 37575.0)
    .drop("max_claimed_amount_sum_3_prior_check")
)

sequenced = (
    per_group
    .select(
        "*",
        F.rank().over(w_distinct_policy_ids_policy_i).alias("second_reported_at_to_date_reported_at_dense_ran"),
        (F.col("cnt") - F.lag("cnt").over(w_distinct_policy_ids_policy_i_2)).alias("cnt_change"),
        F.round(F.avg("cnt").over(w_distinct_policy_ids_policy_i_3), 2).alias("cnt_avg_1_prior"),
    )
)

result = (
    sequenced
    .filter(F.col("second_reported_at_to_date_reported_at_dense_ran") <= 6)
    .select(
        "policy_id",
        "distinct_policy_ids",
        "cnt",
        "next3_claimed_amount_to_second_reported_at_to_da",
        "second_reported_at_to_date_reported_at_dense_ran_2",
        "second_reported_at_to_date_reported_at_dense_ran",
        "cnt_avg_1_prior",
        F.greatest(F.col("cnt_avg_1_prior"), F.col("distinct_policy_ids")).alias("greatest_cnt_avg_1_prior_distinct_policy_ids"),
    )
    .orderBy(
        F.col("next3_claimed_amount_to_second_reported_at_to_da").asc_nulls_last(),
    )
)
