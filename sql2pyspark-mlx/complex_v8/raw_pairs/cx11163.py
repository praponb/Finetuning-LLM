from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_moving_2 = Window.orderBy("reported_at", "id").rowsBetween(-2, Window.currentRow)
w_policy_id_status_by_claim_da = Window.partitionBy("policy_id", "status").orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_status_claim_type = Window.partitionBy("status", "claim_type")
w_all_by_reported_at_prior_2 = Window.orderBy("reported_at", "id").rowsBetween(-2, -1)
w_policy_id_status_by_claim_da_2 = Window.partitionBy("policy_id", "status").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_approved_amount_movin = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_policy_id_status_by_claim_da_3 = Window.partitionBy("policy_id", "status").orderBy(F.col("claim_date").asc_nulls_last())
w_policy_id_status_by_claim_da_4 = Window.partitionBy("policy_id", "status").orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_status_by_approve = Window.partitionBy("claim_type", "status").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(-6, -1)
w_status_by_next_approved_amou = Window.partitionBy("status").orderBy(F.col("next_approved_amount_bucket").asc_nulls_last())
w_all_by_claim_date_percent_ra = Window.orderBy(F.col("claim_date_percent_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_percent_ra_2 = Window.orderBy(F.col("claim_date_percent_rank").desc(), "id")
w_status_by_next_approved_amou_2 = Window.partitionBy("status").orderBy(F.col("next_approved_amount_bucket").asc_nulls_last(), "id").rowsBetween(-2, Window.currentRow)
w_status_by_next_approved_amou_3 = Window.partitionBy("status").orderBy(F.col("next_approved_amount_bucket").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id = Window.partitionBy("policy_id")

filtered_claims = (
    df_claims
    .filter(F.col("adjuster_id") >= 7)
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
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
    )
)

windowed = (
    filtered_claims
    .select(
        "*",
        F.round(F.avg("claimed_amount").over(w_all_by_reported_at_moving_2), 2).alias("claimed_amount_avg_3"),
        F.lead("approved_amount_bucket").over(w_policy_id_status_by_claim_da).alias("next_approved_amount_bucket"),
        F.round(F.col("approved_amount_bucket") - F.avg("approved_amount_bucket").over(w_status_claim_type), 2).alias("approved_amount_bucket_vs_status_claim_type_avg"),
        F.max("approved_amount").over(w_all_by_reported_at_prior_2).alias("approved_amount_max_2_prior"),
        F.round(F.avg("claimed_amount").over(w_policy_id_status_by_claim_da_2), 2).alias("claimed_amount_avg_7"),
        F.round(F.avg("claimed_amount").over(w_all_by_approved_amount_movin), 2).alias("claimed_amount_avg_7_2"),
        F.row_number().over(w_all_by_reported_at).alias("seq_no"),
        F.round(F.percent_rank().over(w_policy_id_status_by_claim_da_3), 3).alias("claim_date_percent_rank"),
        F.last("approved_amount", ignorenulls=True).over(w_policy_id_status_by_claim_da_4).alias("last_known_approved_amount"),
        F.lead("claimed_amount", 3).over(w_all_by_reported_at).alias("next3_claimed_amount"),
        F.round(F.cume_dist().over(w_policy_id_status_by_claim_da), 3).alias("claim_date_cume_dist"),
        F.min("approved_amount_bucket").over(w_status_claim_type).alias("status_claim_type_min_approved_amount_bucket"),
        F.sum("approved_amount_bucket").over(w_claim_type_status_by_approve).alias("approved_amount_bucket_sum_6_prior"),
        F.size(F.collect_set("adjuster_id").over(w_status_claim_type)).alias("distinct_adjuster_id_in_status_claim_type"),
    )
)

labeled = (
    windowed
    .select(
        "*",
        F.concat_ws(": ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
    )
)

with_trends = (
    labeled
    .select(
        "*",
        F.round(F.cume_dist().over(w_status_by_next_approved_amou), 3).alias("next_approved_amount_bucket_cume_dist"),
        F.first("reported_at", ignorenulls=True).over(w_all_by_claim_date_percent_ra).alias("first_known_reported_at"),
        F.dense_rank().over(w_all_by_claim_date_percent_ra_2).alias("claim_date_percent_rank_dense_rank"),
        F.dense_rank().over(w_status_by_next_approved_amou).alias("next_approved_amount_bucket_dense_rank"),
        F.sum("claimed_amount").over(w_status_by_next_approved_amou_2).alias("claimed_amount_sum_3"),
        F.last("next3_claimed_amount").over(w_status_by_next_approved_amou_3).alias("last_next3_claimed_amount"),
        F.sum("next3_claimed_amount").over(w_status_by_next_approved_amou_2).alias("next3_claimed_amount_sum_3"),
        F.size(F.collect_set("status").over(w_policy_id)).alias("distinct_status_in_policy_id"),
    )
)

totals = (
    with_trends
    .groupBy("reported_at_dayofweek")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("status").alias("distinct_status"),
        F.round(F.sum("claim_date_percent_rank_dense_rank") / F.nullif(F.sum("claimed_amount_sum_3"), F.lit(0)), 3).alias("claim_date_percent_rank_dense_rank_to_claimed_am"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    totals
    .select(
        "reported_at_dayofweek",
        "n_rows",
        "distinct_status",
        "claim_date_percent_rank_dense_rank_to_claimed_am",
        F.greatest(F.col("reported_at_dayofweek"), F.col("n_rows")).alias("greatest_reported_at_dayofweek_n_rows"),
        F.greatest(F.col("reported_at_dayofweek"), F.col("distinct_status")).alias("greatest_reported_at_dayofweek_distinct_status"),
    )
    .orderBy("distinct_status")
)
