from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_claim_date_year = Window.partitionBy("status", F.year("claim_date"))
w_policy_start_date_month_stat = Window.partitionBy(F.month("policy_start_date"), "status").orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_policy_start_date_ful = Window.orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_claim_type_by_poli = Window.partitionBy("policy_id", "claim_type").orderBy("policy_start_date", "id")
w_policy_end_date_year_by_poli = Window.partitionBy(F.year("policy_end_date")).orderBy(F.col("policy_start_date").desc())
w_policy_start_date_month_stat_2 = Window.partitionBy(F.month("policy_start_date"), "status").orderBy(F.col("policy_start_date").desc(), "id")
w_policy_end_date_year_by_poli_2 = Window.partitionBy(F.year("policy_end_date")).orderBy(F.col("policy_start_date").desc(), "id")
w_policy_end_date_year_by_poli_3 = Window.partitionBy(F.year("policy_end_date")).orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_policy_start_date_run = Window.orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_prev3_policy_end_d = Window.partitionBy("status").orderBy(F.col("prev3_policy_end_date").desc(), "id")
w_status_claim_date_month_by_c = Window.partitionBy("status", F.month("claim_date")).orderBy(F.col("claimed_amount_change").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_start_date_year = Window.partitionBy(F.year("policy_start_date"))
w_status_claim_date_month_by_c_2 = Window.partitionBy("status", F.month("claim_date")).orderBy(F.col("claimed_amount_change").desc_nulls_first(), "id")
w_status_by_prev3_policy_end_d_2 = Window.partitionBy("status").orderBy(F.col("prev3_policy_end_date").desc(), "id").rowsBetween(-3, -1)

recent = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")) & (F.col("claim_type").isin("medical", "theft", "delay")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

with_parent = (
    recent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

with_trends = (
    with_parent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "policy_end_date",
        "policy_start_date",
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_status_claim_date_year), F.lit(0)), 4).alias("claimed_amount_share_of_status_claim_date_year"),
        F.round(F.avg("claimed_amount").over(w_policy_start_date_month_stat), 2).alias("claimed_amount_avg_7"),
        F.nth_value("policy_end_date", 2).over(w_all_by_policy_start_date_ful).alias("second_policy_end_date"),
        F.last("claimed_amount", ignorenulls=True).over(w_all_by_claimed_amount_runnin).alias("last_known_claimed_amount"),
        F.min("claimed_amount").over(w_policy_id).alias("policy_id_min_claimed_amount"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_policy_id_claim_type_by_poli)).alias("claimed_amount_change"),
        F.dense_rank().over(w_policy_end_date_year_by_poli).alias("policy_start_date_dense_rank"),
        F.lead("claim_date", 3).over(w_policy_start_date_month_stat_2).alias("next3_claim_date"),
        F.lag("policy_end_date", 3).over(w_policy_end_date_year_by_poli_2).alias("prev3_policy_end_date"),
        F.last("claimed_amount", ignorenulls=True).over(w_policy_end_date_year_by_poli_3).alias("last_known_claimed_amount_2"),
        F.last("claim_date", ignorenulls=True).over(w_all_by_claimed_amount_runnin).alias("last_known_claim_date"),
        F.max("claimed_amount").over(w_all_by_policy_start_date_run).alias("max_claimed_amount_so_far"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "claimed_amount_avg_7",
        "second_policy_end_date",
        "last_known_claimed_amount",
        "policy_id_min_claimed_amount",
        "policy_start_date_dense_rank",
        "last_known_claimed_amount_2",
        "last_known_claim_date",
        "max_claimed_amount_so_far",
        F.round(F.cume_dist().over(w_status_by_prev3_policy_end_d), 3).alias("prev3_policy_end_date_cume_dist"),
        F.nth_value("prev3_policy_end_date", 2).over(w_status_claim_date_month_by_c).alias("second_prev3_policy_end_date"),
        F.sum("max_claimed_amount_so_far").over(w_policy_start_date_year).alias("policy_start_date_year_sum_max_claimed_amount_so"),
        F.row_number().over(w_status_claim_date_month_by_c_2).alias("rn"),
        F.round(F.avg("policy_id_min_claimed_amount").over(w_status_by_prev3_policy_end_d_2), 2).alias("policy_id_min_claimed_amount_avg_3_prior"),
        F.size(F.collect_set("adjuster_id").over(w_policy_start_date_year)).alias("distinct_adjuster_id_in_policy_start_date_year"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "claimed_amount_avg_7",
        "second_policy_end_date",
        "last_known_claimed_amount",
        "last_known_claimed_amount_2",
        "last_known_claim_date",
        "max_claimed_amount_so_far",
        "prev3_policy_end_date_cume_dist",
        "second_prev3_policy_end_date",
        "rn",
        "policy_id_min_claimed_amount_avg_3_prior",
        "distinct_adjuster_id_in_policy_start_date_year",
    )
    .orderBy(F.col("prev3_policy_end_date_cume_dist").desc())
)
